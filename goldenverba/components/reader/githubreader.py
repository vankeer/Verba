import base64
import json
import os
from datetime import datetime
import urllib.parse

import requests
from wasabi import msg

from goldenverba.components.reader.document import Document
from goldenverba.components.reader.interface import InputForm, Reader
from goldenverba.components.reader.unstructuredpdf import UnstructuredPDF
from goldenverba.components.reader.pdfreader import PDFReader


class GithubReader(Reader):
    """
    The GithubReader downloads files from Github and ingests them into Weaviate.
    """

    def __init__(self):
        super().__init__()
        self.name = "GithubReader"
        self.requires_env = ["GITHUB_TOKEN"]
        self.description = "Downloads only text files from a GitHub repository and ingests it into Verba. Use this format {owner}/{repo}/{branch}/{folder}"
        self.input_form = InputForm.INPUT.value
        self.pdf_reader = PDFReader()
        self.unstructured_pdf = UnstructuredPDF()
        self.supported_file_types = (".md", ".mdx", ".txt", ".json")
        if os.environ.get("UNSTRUCTURED_API_URL") or os.environ.get("UNSTRUCTURED_API_KEY"):
            self.supported_file_types += (".pdf", ".epub",)

    def load(
        self,
        bytes: list[str] = None,
        contents: list[str] = None,
        paths: list[str] = None,
        fileNames: list[str] = None,
        document_type: str = "Documentation",
    ) -> list[Document]:
        """Ingest data into Weaviate
        @parameter: bytes : list[str] - List of bytes
        @parameter: contents : list[str] - List of string content
        @parameter: paths : list[str] - List of paths to files
        @parameter: fileNames : list[str] - List of file names
        @parameter: document_type : str - Document type
        @returns list[Document] - Lists of documents.
        """
        if fileNames is None:
            fileNames = []
        if paths is None:
            paths = []
        if contents is None:
            contents = []
        if bytes is None:
            bytes = []
        documents = []

        # If paths exist
        if len(paths) > 0:
            for path in paths:
                if path != "":
                    files = self.fetch_docs(path)

                    for _file in files:
                        try:
                            filename = _file.split("/")[-1]
                            msg.info(f"Loading {_file}")
                            content, link, _path = self.download_file(path, _file)
                            msg.info(f"Downloaded {_path}")
                        except Exception as e:
                            msg.warn(f"Couldn't load, skipping {_file}: {str(e)}")
                            continue

                        if filename.endswith(".pdf") or filename.endswith(".epub"):
                            msg.info(f"Reading PDF {filename}")
                            # Use UnstructuredPDF to process PDF content
                            try:
                                parsed_docs = self.unstructured_pdf.load_bytes(
                                    content,
                                    filename,
                                    document_type
                                )
                                msg.info(f"Loaded {len(parsed_docs)} documents with UnstructuredPDF")
                                for doc in parsed_docs:
                                    documents.append(doc)
                            except Exception as e:
                                msg.warn(f"Couldn't load PDF with Unstructured, trying with normal PDF reader for {_file}: {str(e)}")
                                try:
                                    # Decode the base64 content to binary
                                    pdf_bytes = base64.b64decode(content)
                                    # Save the decoded content to a temporary file
                                    temp_file_path = "temp_" + filename
                                    with open(temp_file_path, 'wb') as f:
                                        f.write(pdf_bytes)
                                    # Load the PDF using PDFReader
                                    parsed_docs = self.pdf_reader.load(paths=[temp_file_path])
                                    msg.info(f"Loaded {len(parsed_docs)} documents with PDFReader")
                                    for doc in parsed_docs:
                                        documents.append(doc)
                                except Exception as e:
                                    msg.warn(f"Skipping; couldn't load PDF with normal PDF reader for {_path}: {str(e)}")
                                    continue
                        elif filename.endswith(".json"):
                            msg.info(f"Reading JSON {filename}")
                            json_obj = json.loads(str(content))
                            try:
                                document = Document.from_json(json_obj)
                                documents.append(document)
                            except Exception as e:
                                raise Exception(f"Loading JSON failed {e}")

                        else:
                            msg.info(f"Reading document {filename}")
                            document = Document(
                                text=content,
                                type=document_type,
                                name=_file,
                                link=link,
                                path=_path,
                                timestamp=str(
                                    datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                                ),
                                reader=self.name,
                            )
                            documents.append(document)

        msg.good(f"Loaded {len(documents)} documents")
        return documents

    def fetch_docs(self, path: str) -> list:
        """Fetch filenames from Github
        @parameter path : str - Path to a GitHub repository
        @returns list - List of document names.
        """
        split = path.split("/")
        owner = split[0]
        repo = split[1]
        branch = split[2] if len(split) > 2 else "main"
        folder_path = "/".join(split[3:]) if len(split) > 3 else ""

        url = f"https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"
        headers = {
            "Authorization": f"token {os.environ.get('GITHUB_TOKEN', '')}",
            "Accept": "application/vnd.github.v3+json",
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()  # Raise an exception for HTTP errors

        files = [
            item["path"]
            for item in response.json()["tree"]
            if item["path"].startswith(folder_path)
            and (
                item["path"].endswith(".md")
                or item["path"].endswith(".mdx")
                or item["path"].endswith(".txt")
                or item["path"].endswith(".json")
                or item["path"].endswith(".pdf")
                or item["path"].endswith(".epub")
            )
        ]
        msg.info(
            f"Fetched {len(files)} filenames from {url} (checking folder {folder_path})"
        )
        return files

    def download_file(self, path: str, file_path: str) -> str:
        """Download files from Github based on filename
        @parameter path : str - Path to a GitHub repository
        @parameter file_path : str - Path of the file in repo
        @returns str - Content of the file.
        """
        split = path.split("/")
        owner = split[0]
        repo = split[1]
        branch = split[2] if len(split) > 2 else "main"

        if not isinstance(file_path, str):
            raise ValueError(f"file_path must be a string, got {type(file_path)} instead: {file_path}")

        encoded_file_path = urllib.parse.quote(file_path.encode("utf-8"), safe="")

        url = f"https://api.github.com/repos/{owner}/{repo}/contents/{encoded_file_path}?ref={branch}"
        headers = {
            "Authorization": f"token {os.environ.get('GITHUB_TOKEN', '')}",
            "Accept": "application/vnd.github.v3+json",
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        response_json = response.json()
        # msg.info(f"Response JSON: {response_json}")

        # if file is a PDF, encode the PDF content to base64
        if file_path.endswith(".pdf") or file_path.endswith(".epub"):
            download_url = response_json["download_url"]
            # Download the bytes string from the download_url as content
            download_response = requests.get(download_url)
            download_response.raise_for_status()
            msg.info(f"Downloaded {download_url}")
            content_b64 = base64.b64encode(download_response.content)
            content = content_b64.decode("utf-8")
            msg.info(f"Content starts with: {content[:20]}")
        else:
            content_b64 = response_json["content"]
            content = base64.b64decode(content_b64).decode("utf-8")

        link = response_json["html_url"]
        path = response_json["path"]
        msg.info(f"Downloaded {url}")
        return (content, link, path)

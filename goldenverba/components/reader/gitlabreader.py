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


class GitLabReader(Reader):
    """
    The GitLabReader downloads files from GitLab and ingests them into Weaviate.
    """

    def __init__(self):
        super().__init__()
        self.name = "GitLabReader"
        self.requires_env = ["GITLAB_TOKEN"]
        self.description = "Downloads only text files from a GitLab repository and ingests it into Verba. Use this format {project_id}/{branch}/{folder}"
        self.input_form = InputForm.INPUT.value
        self.pdf_reader = PDFReader()
        self.unstructured_pdf = UnstructuredPDF()
        self.supported_file_types = (".md", ".mdx", ".txt", ".json")
        if os.environ.get("UNSTRUCTURED_API_URL") or os.environ.get("UNSTRUCTURED_API_KEY"):
            self.supported_file_types += (".pdf",)

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

        if paths:
            for path in paths:
                try:
                    files = self.fetch_docs(path)
                except Exception as e:
                    msg.fail(f"Couldn't fetch, skipping {path}: {str(e)}")
                    continue

                for _file in files:
                    try:
                        content, link, _path = self.download_file(path, _file["path"])
                        document = None
                    except Exception as e:
                        msg.warn(f"Couldn't load, skipping {_file}: {str(e)}")
                        continue

                    filename = _path.split("/")[-1]
                    if _path.endswith(".pdf"):
                        msg.info(f"Reading PDF {_path}")
                        # Use UnstructuredPDF to process PDF content
                        try:
                            parsed_docs = self.unstructured_pdf.load_bytes(
                                content,
                                filename,
                                document_type
                            )
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
                                for doc in parsed_docs:
                                    documents.append(doc)
                            except Exception as e:
                                msg.warn(f"Skipping; couldn't load PDF with normal PDF reader for {_path}: {str(e)}")
                                continue
                    elif _path.endswith(".json"):
                        msg.info(f"Reading JSON {_path}")
                        json_obj = json.loads(content)
                        document = Document.from_json(json_obj)
                        documents.append(document)
                    else:
                        msg.info(f"Reading document {_path}")
                        document = Document(
                            text=content,
                            type=document_type,
                            name=filename,
                            link=link,
                            path=_path,
                            timestamp=str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                            reader=self.name,
                        )
                        documents.append(document)

        msg.good(f"Loaded {len(documents)} documents")
        return documents

    def fetch_docs(self, path: str, project_id=None, branch=None) -> list:
        if project_id is None or branch is None:
            project_id, branch, folder_path = self._parse_path(path)
        else:
            folder_path = path

        files = []
        try:
            # Ensure the folder path is a string before encoding
            if not isinstance(folder_path, str):
                raise ValueError(f"folder_path must be a string, got {type(folder_path)} instead: {folder_path}")
        
            encoded_folder_path = urllib.parse.quote(folder_path.encode("utf-8"), safe="")
            url = f"https://gitlab.com/api/v4/projects/{project_id}/repository/tree?ref={branch}&path={encoded_folder_path}&per_page=100"
            headers = {
                "Authorization": f"Bearer {os.environ.get('GITLAB_TOKEN', '')}",
            }
            response = requests.get(url, headers=headers)
            response.raise_for_status()

            items = response.json()
            files = []
            for item in items:
                if item["type"] == "blob" and item["path"].endswith(self.supported_file_types):
                    msg.info(f"Found file: {item['path']}")
                    files.append({"path": item["path"], "project_id": project_id, "branch": branch})
                elif item["type"] == "tree":
                    # Ensure that item["path"] is a string before recursively calling fetch_docs
                    if isinstance(item["path"], str):
                        msg.info(f"Exploring folder: {item['path']}")
                        files.extend(self.fetch_docs(item["path"], project_id, branch))
                    else:
                        raise TypeError(f"Expected a string path for recursive fetch_docs call, got {type(item['path'])} instead: {item['path']}")

            msg.info(
                f"Fetched {len(files)} filenames from {url} (checking folder {folder_path})"
            )
        except Exception as e:
            msg.fail(f"Couldn't fetch {path}: {str(e)}")

        return files

    def download_file(self, path: str, file_path: str) -> str:
        project_id, branch, _ = self._parse_path(path)
        msg.info(f"Downloading {file_path} from {project_id}/{branch}")

        if not isinstance(file_path, str):
            raise ValueError(f"file_path must be a string, got {type(file_path)} instead: {file_path}")

        encoded_file_path = urllib.parse.quote(file_path.encode("utf-8"), safe="")

        url = f"https://gitlab.com/api/v4/projects/{project_id}/repository/files/{encoded_file_path}/raw?ref={branch}"
        headers = {
            "Authorization": f"Bearer {os.environ.get('GITLAB_TOKEN', '')}",
        }
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        # if file is a PDF, encode the PDF content to base64
        if file_path.endswith(".pdf"):
            encoded_content = base64.b64encode(response.content)
            content = encoded_content.decode('utf-8')
        else:
            content = response.text

        link = f"https://gitlab.com/{project_id}/-/blob/{branch}/{file_path}"
        msg.info(f"Downloaded {url}")
        return (content, link, file_path)

    def _parse_path(self, path: str):
        split = path.split('/')
        project_id = split[0]
        branch = split[1]
        folder_path = '/'.join(split[2:]) if len(split) > 2 else ""
        return project_id, branch, folder_path

import cgi
import uuid
import base64

def decode(event):
    body = event["body"]
    if event["isBase64Encoded"]:
        body = base64.b64decode(event["body"])

    headers = {
        "content-type": event["headers"]["Content-Type"],
        "content-length": len(body)
    }

    form = cgi.FieldStorage(
        cgi.BytesIO(body),
        headers=headers,
        environ={'REQUEST_METHOD': 'POST'}
    )

    if "file" not in form:
        raise RuntimeError("No file field in submitted form")

    files = []
    for value in form.getlist("file"):
        filename = str(uuid.uuid4()) + ".csv"
        files.append(CsvFile(filename, value))

    return files

class CsvFile:
    def __init__(self, filename, bytesStr):
        self._filename = filename
        self._bytesStr = bytesStr

    @property
    def filename(self):
        return self._filename

    @property
    def bytesStr(self):
        return self._bytesStr

import mimetypes
from click import FileError

def to_content_type(filename:str): 
    content_type = mimetypes.guess_type(filename)[0] 
    if content_type is None:
        raise FileError(f'file with {filename} aneh')
    
    return content_type 
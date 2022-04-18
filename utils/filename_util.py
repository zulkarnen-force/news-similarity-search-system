import mimetypes
from click import FileError

def get_content_type(filename:str):
        
    content_type = mimetypes.guess_type(filename)[0] 
    if content_type is None:
        raise FileError(f'filename: {filename} not valid')
    
    return content_type 
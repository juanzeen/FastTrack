class FastTrackError(Exception):
    pass

class NetworkError(FastTrackError):
    pass

class ProtocolError(FastTrackError):
    pass

class StorageError(FastTrackError):
    pass

class FileOperationError(FastTrackError):
    pass

class DownloadError(NetworkError):
    pass

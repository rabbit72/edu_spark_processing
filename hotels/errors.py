class ClusterError(Exception):
    pass


class YarnError(ClusterError):
    pass


class HdfsError(Exception):
    pass

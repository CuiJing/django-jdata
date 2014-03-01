#_*_coding:utf-8 _*_

from django_jdata.utils import return_http_json


class BaseError(Exception):
    def to_django_response(self):
        try:
            error = eval(self.__str__())
        except:
            error = self.__str__()
        return return_http_json({'code':self.errcode,'error':error})

class ObjectInitError(BaseError):
    errcode = 1400

class ObjectNotFound(BaseError):
    errcode = 1404

class ObjectPKNotFound(BaseError):
    errcode = 1402

class ObjectConfigError(BaseError):
    errcode = 1405

class ObjectConfigTestError(BaseError):
    errcode = 1406

class URLParameterError(BaseError):
    errcode = 1401
    
class TableNotExists(Exception):
    errcode = 2401

class TableNotExistsInDMC(Exception):
    errcode = 2402

class TableNameIsRequried(Exception):
    errcode = 2403

class TableAlreadyExists(Exception):
    errcode = 2404

class UploadDataMethodError(BaseError):
    errcode = 3401

class UploadDataParameterError(BaseError):
    errcode = 3401

class SpecifiedTimeTooShort(BaseError):
    errcode = 4001

class AccessDenied(BaseError):
    errcode = 1403

class DebugPrint(BaseError):
    errcode = 1000

class UnsupportedQuery(BaseError):
    errcode = 1407

"""
   Module to custom errors handling and additional functionality.
"""
from botocore.exceptions import ClientError, EndpointConnectionError
from grapher.core.constants import UNAUTHORIZED


class ExceptionWrapper:
    """Wraps error messages to adjust them to one standard"""
    # TODO: add time and logger integration

    _error_messages = {
        BaseException: UNAUTHORIZED,
        ClientError: 'Botocore library error',
        EndpointConnectionError: 'Error connection to endpoint',
        KeyError:' Key error occured'
    } # default messages for each exception type

    def __init__(self, exception_type=BaseException, msg='', *args, **kwargs):
        if msg == '':
            self._wrapped_message = self._error_messages.get(type(exception_type), 'The exceptional situation occured')
        else: 
            self._wrapped_message = msg

    def print_error(self):
        # TODO: it possible to add more verbosity levels
        if self._wrapped_message:
            print(self._wrapped_message)   

    def wrap(self):
        self.print_error() 
        return {'error': self._wrapped_message}

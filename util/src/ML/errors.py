""""Custom errors for this module."""


class Error(Exception):
    """"Base Error."""

    def __init__(self, error_message, _type='400'):
        """Create a new Error.

        Args:
          error_message: String.
          _type: String in {'400', 'info'}. Determines return type.
        """
        self.error_message = error_message
        self._type = _type


class FileNotFound(Error):
    """An expected file was not found."""
    pass


class ModuleNotFinished(Error):

    def __init__(self, student_pin, class_code, unit_name, module_name):
        error_message = 'Student %s in class %s has not completed all ' \
                        'activities in unit %s module %s.' \
                        % (student_pin, class_code, unit_name, module_name)
        super(ModuleNotFinished, self).__init__(error_message, _type='info')


class MoreThanOneRecord(Error):
    """A query expecting one record returned more than one."""
    pass


class NoCompletedModules(Error):

    def __init__(self, student_pin, class_code):
        error_message = 'Student %s in class %s has no completed modules.' \
                        % (student_pin, class_code)
        super(NoCompletedModules, self).__init__(error_message, _type='info')


class NoRecord(Error):
    """A query expecting one record returned none."""
    pass

"""Utilities for managing curriculum information."""
import json
import os
from .. import errors


FULL_NAME_MAP = {
    'P': 'preschool',
    'K': 'kindergarten',
    'N': 'nursery'
}


def check(level):
    """Check the level is in the expected set.

    Raises:
      ValueError if level not in the expected set.
    """
    if level not in {'P', 'K', 'N'}:
        raise ValueError('Unexpected level %r' % level)


def get_curriculum(level, version='20.0.0'):
    """Load curriculum information.

    Args:
      level: String in {'K', 'P', 'N'}.
      version: String.

    Returns:
      Dictionary.

    Raises:
      ValueError if level not in the expected set.
    """
    check(level)
    file_path = get_file_path(level, version)
    return json.loads(open(file_path).read())


def get_file_path(level, version='20.0.0'):
    """Determine the file path for a curriculum information file.

    TODO: don't need version arg - current is all.

    We expect curriculum files to be in the curriculums folder and to have a
    name following the pattern:
      curriculum-full_name-version_no.json
    For example:
      curriculum-kindergarten-18.0.0.json
    If encountering a FileNotFound error check the file is in the folder and
    that it has a file name following this convention.

    Args:
      level: String in {'K', 'P', 'N'}.
      version: String.

    Returns:
      String.

    Raises:
      ValueError if level not in the expected set.
      FileNotFound if the expected file does not exist.
    """
    check(level)

    cwd = os.getcwd()
    if cwd.endswith('schools-cloud/python'):  # local testing
        sub_folder = 'src/util/curriculums/'
    elif cwd == '/var/task':  # production environment
        sub_folder = 'util/curriculums/'
    else:  # some kind of weird situation - raise an error
        raise Exception('Unexpected environment cwd: %r' % cwd)

    level = FULL_NAME_MAP[level]
    folder = os.path.join(cwd, sub_folder)
    if version in ['19.0.0', '20.0.0']:
        file_path = os.path.join(folder, 'curriculum-%s-gb-%s.json'
                                 % (level, version))
    else:
        file_path = os.path.join(folder, 'curriculum-%s-%s.json'
                                 % (level, version))

    if not os.path.exists(file_path):
        raise errors.FileNotFound(file_path)

    return file_path


# TODO: curriculum 18 stuff, should hopefully be able to delete one day?
def clean(route):
    """Clean a route string.

    Essentially remove everything after the question mark.

    Args:
      route: String.

    Returns:
      String.
    """
    if '?' in route:
        route = route.split('?')[0]
    return route


def mod_from_route(route):
    """Pulls the module out from the route.

    E.g. '/lesson/phonics/activity/tree/rank/1' returns 'tree'.
    """
    return route.split('/rank')[0].split('/')[-1]


def required_routes(curriculum, module):
    """Determine required routes for a unit's module.

    Args:
      curriculum: Json. The full thing.
      module: Dictionary.

    Returns:
      List of strings.
    """
    activity_indices = module['activities']
    activities = [curriculum['activityStore'][ix]
                  for ix in activity_indices]
    # filter out flashcards
    activities = [a for a in activities
                  if a['icon']['activity'] != 'flashcards']
    routes = [a['route'] for a in activities]
    return [clean(r) for r in routes]


def required_routes18(module):
    routes = [a['route'] for a in module['activities']]
    routes = [r for r in routes if 'flashcards' not in r]
    return [clean(r) for r in routes]


def get_module(curriculum, unit_number, module_number):
    """Get a module for a unit in a curriculum.

    Args:
      curriculum: Dictionary.
      unit_number: Integer.
      module_number: Integer.

    Returns:
      Dictionary.

    Raises:
      ValueError if unit_number-module_number not in curriculum.
    """
    unit = next((u for u in curriculum['units']
                 if u['unit'] == int(unit_number)), None)
    if unit is None:
        raise ValueError('No unit %s in curriculum %s'
                         % (unit_number, curriculum['name']))
    module = next((m for m in unit['modules']
                   if m['index'] == int(module_number)), None)
    if module is None:
        raise ValueError('No module %s in unit %s of curriculum %s.'
                         % (module_number, unit_number, curriculum['name']))
    return module


def get_module18(curriculum, unit_number, module_number):
    """Get a module for a unit in a curriculum.

    Args:
      curriculum: Dictionary.
      unit_number: Integer.
      module_number: Integer.

    Returns:
      Dictionary.

    Raises:
      ValueError if unit_number-module_number not in curriculum.
    """
    unit = next((u for u in curriculum['units']
                if u['index'] == int(unit_number)), None)
    if unit is None:
        raise ValueError('No unit %s in curriculum %s'
                         % (unit_number, curriculum['name']))
    module = next((m for m in unit['modules']
                   if m['index'] == int(module_number)), None)
    if module is None:
        raise ValueError('No module %s in unit %s of curriculum %s.'
                         % (module_number, unit_number, curriculum['name']))
    return module

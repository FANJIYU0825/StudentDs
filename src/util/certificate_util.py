"""Utilities for certificates."""
from . import py_util, config, LOGGER


def filter_best(experiences):
    mods = list(set([e['mod'] for e in experiences]))
    best = []
    for mod in mods:
        mod_experiences = [e for e in experiences if e['mod'] == mod]
        best_score = max(e['scr'] for e in mod_experiences)
        best_experience = next(e for e in mod_experiences
                               if e['scr'] == best_score)
        best.append(best_experience)
    return best


def get_certificate(student, _class, experiences):
    experiences = filter_best(experiences)
    avg_score = py_util.average(
        sigma=sum(e['scr'] for e in experiences),
        n=len(experiences))
    certificate_score = shape_score(avg_score)
    top_k_words = get_top_k_words(experiences)
    result = {
        'name': student['name'] if 'name' in student.keys() else '',
        'username': student['username'],
        'class_level': _class['level'],
        'average_score': certificate_score,
        'content': top_k_words,
        'class_name': _class['name'],
        'teacher_name': _class['username']
    }
    LOGGER.info('Returning result:')
    LOGGER.info(result)
    return result


def get_cls_loc(class_code, level, unit_name, module_name):
    if len(unit_name) < 2:
        unit_name = '0' + unit_name
    if len(module_name) < 2:
        module_name = '0' + module_name
    # return '%s_%s%s%s' % (class_code, level, unit_name, module_name)
    return f'{class_code}_{level} {unit_name} {module_name}'


def is_complete(level, unit_name, module_name, experiences):
    return 'quiz' in list(set(e['mod'].lower() for e in experiences))


def is_complete18(curriculum, unit, module, experiences):
    return 'quiz' in list(set(e['mod'].lower() for e in experiences))


def shape_score(avg_score):
    """Shape an average score for a unit-module.

    The function floors at 50 and is piecewise affine to hit the following
    targets:
      70 -> 80
      80 -> 10
    """
    if avg_score <= 50:
        score = 50
    elif 50 < avg_score < 70:
        score = (30 / float(20)) * avg_score - 25
    elif 70 <= avg_score < 80:
        score = (20 / float(10)) * avg_score - 60
    else:  # 80 <= avg_score:
        score = 100
    return int(round(score, 0))


def get_top_k_words(experiences):
    experiences = py_util.flatten([e['exp'] for e in experiences])
    word_scores = {}

    for e in experiences:
        word = e['w']
        if word not in word_scores.keys():
            word_scores[word] = {
                'questions': 0,
                'correct': 0,
                'experiences': 0}
        if e['x'] == 'A':
            word_scores[word]['questions'] += 1
            if 'm' not in e.keys():
                word_scores[word]['correct'] += 1
        elif e['x'] == 'X':
            word_scores[word]['experiences'] += 1
        else:
            erro = e["x"]
            raise ValueError(f'Unexpected activity type {erro}' )

    # calculate scores
    for word in word_scores.keys():
        correct = word_scores[word]['correct']
        incorrect = word_scores[word]['questions'] - correct
        experiences = word_scores[word]['experiences']
        word_scores[word]['score'] = 10 * correct - 10 * incorrect + experiences

    # sorts highest score to lowest
    ordered_words = list(reversed(sorted(
        word_scores.items(),
        key=lambda s: s[1]['score'])))

    # grab top k
    k = config.STUDENT_LAST_TOP_K_WORDS
    top_k = ordered_words[0:k]

    return [w[0] for w in top_k]

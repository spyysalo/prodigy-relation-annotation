import datetime
import prodigy

from prodigy.components.loaders import JSONL


RELATION_TYPES = [
    'employee',
    'owner',
    'founder',
    'NONE',
]


def iso8601_now():
    """Return current time in ISO 8601 format w/o microseconds."""
    return datetime.datetime.now().replace(microsecond=0).isoformat(' ')


def count_lines(file_path):
    return sum(1 for i in open(file_path))


@prodigy.recipe(
    'relations',
    dataset=('The dataset to save to', 'positional', None, str),
    file_path=('Path to texts', 'positional', None, str),
    annotator=('Annotator name', 'positional', None, str),
)
def relations(dataset, file_path, annotator):
    """Annotate the sentiment of texts using different mood options."""
    stream = JSONL(file_path)     # load in the JSONL file

    # TODO need to remove previously annotated
    total_lines = count_lines(file_path)
    def progress(controller, update_return_value):
        return controller.total_annotated / total_lines

    def add_label(stream):
        for task in stream:
            task['label'] = f'({task["mention1"]}, {task["mention2"]})'
            yield task
    stream = add_label(stream)
    stream = add_options(stream)

    def before_db(examples):
        for e in examples:
            if 'created' not in e:
                e['created'] = iso8601_now()
            if 'annotator' not in e:
                e['annotator'] = annotator
        return examples

    return {
        'dataset': dataset,
        'stream': stream,
        'view_id': 'choice',
        'progress': progress,
        'before_db': before_db,
    }


def add_options(stream):
    options = [
        { 'id': t, 'text': t } for t in RELATION_TYPES
    ]
    for task in stream:
        task['options'] = options
        yield task

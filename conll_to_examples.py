#!/usr/bin/env python3

import sys
import json
import html
import uuid
import random

from argparse import ArgumentParser
from collections import namedtuple


Mention = namedtuple('Mention', 'start end type text')


# Entity type pairs to include in output
TARGET_TYPE_PAIRS = [
    ('PERSON', 'ORG'),
]


def argparser():
    ap = ArgumentParser()
    ap.add_argument('-s', '--seed', default=None, type=int, help='random seed')
    ap.add_argument('conll')
    return ap


def read_conll(stream):
    words, tags = [], []
    for ln, line in enumerate(stream, start=1):
        if line.startswith('#'):
            continue    # comment
        elif line.isspace():
            if words and tags:
                yield words, tags
            words, tags = [], []
        else:
            word, tag = line.rstrip('\n').split('\t')
            words.append(word)
            tags.append(tag)
    if words and tags:
        yield words, tags


def conll_to_mentions(words, tags):
    mentions, offset, start, label = [], 0, None, None
    sentence = ' '.join(words)
    for word, tag in zip(words, tags):
        if tag[0] in 'OB' and start is not None:    # current ends
            end = offset-1
            mentions.append(Mention(start, end, label, sentence[start:end]))
            start, label = None, None
        if tag[0] == 'B':
            start, label = offset, tag[2:]
        elif tag[0] == 'I':
            if start is None:    # I without B, but nevermind
                start, label = offset, tag[2:]
        else:
            assert tag == 'O', 'unexpected tag {}'.format(tag)
        offset += len(word) + 1    # +1 for space
    if start is not None:    # span open at sentence end
        end = offset-1
        mentions.append(Mention(start, end, label, sentence[start:end]))
    return mentions


def target_mention_pairs(words, tags):
    # for quick filter
    target_types = set()
    for t1, t2 in TARGET_TYPE_PAIRS:
        target_types.update({ t1, t2 })

    mentions = conll_to_mentions(words, tags)
    # filter to reduce O(n^2) check
    target_mentions = [m for m in mentions if m.type in target_types]
    pairs = []
    for m1 in target_mentions:
        if not any(m1.type == t1 for t1, t2 in TARGET_TYPE_PAIRS):
            continue
        for m2 in target_mentions:
            if m1 is m2:
                continue
            if any(m1.type == t1 and m2.type == t2
                   for t1, t2 in TARGET_TYPE_PAIRS):
                pairs.append((m1, m2))
    return pairs


def generate_id(words, mention1, mention2):
    text = ' '.join(words)
    return uuid.uuid3(uuid.NAMESPACE_DNS, f'{text}-{mention1}-{mention2}')


def format_mention(m):
    return (f'<span class="mention {m.type}">{m.text}' +
            f'<span class="mention-type">{m.type}</span></span>')


def format_html(words, mention1, mention2):
    text = ' '.join(words)
    if mention1.start < mention2.start:
        first, second = mention1, mention2
    else:
        first, second = mention2, mention1
    before = html.escape(text[:first.start])
    between = html.escape(text[first.end:second.start])
    after = html.escape(text[second.end:])
    first_str = format_mention(first)
    second_str = format_mention(second)
    return f'{before}{first_str}{between}{second_str}{after}'


def main(argv):
    args = argparser().parse_args(argv[1:])
    random.seed(args.seed)
    
    with open(args.conll) as f:
        for words, tags in read_conll(f):
            pairs = target_mention_pairs(words, tags)
            if not pairs:
                continue
            # Take one pair per sentence
            m1, m2 = random.choice(pairs)
            id_ = generate_id(words, m1, m2)
            html = format_html(words, m1, m2)
            data = {
                'html': html,
                'meta': {
                    'source': str(id_),
                },
                'sentence': ' '.join(words),
                'mention1': m1.text,
                'mention2': m2.text,
            }
            print(json.dumps(data))
            

if __name__ == '__main__':
    sys.exit(main(sys.argv))

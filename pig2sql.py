import argparse
import copy
import re
import sqlparse


def resolve(k, recurse=False):
    v = po[k]

    if v.lower().startswith('load '):
        table = re.match("load '(.*?)'", v, flags=re.IGNORECASE).group(1).replace('prodhive.', '')

        return '{table} {k}'.format(**locals())

    elif v.lower().startswith('filter '):
        ref, where_clause = re.match(
                'filter (.*?) by (.*)',
                v, flags=re.IGNORECASE).groups()

        if recurse: ref = resolve(ref, recurse=recurse)

        return '''
        {ref}
        WHERE {where_clause}
        '''.format(**locals())

    elif v.lower().startswith('join '):
        ref1, k1, ref2, k2 = re.match("join (.*?) by \((.*?)\), (.*?) by \((.*?)\)", 
                v, flags=re.IGNORECASE).groups()

        r1,r2 = copy.copy((ref1,ref2))
        if recurse:
            ref1 = resolve(ref1, recurse=recurse)
            ref2 = resolve(ref2, recurse=recurse)

        keys = '\n          AND '.join(['{r1}.{a} = {r2}.{b}'.format(**locals()) for a,b in zip(re.split('\s*,\s*', k1), re.split('\s*,\s*', k2))])

        return '''{ref1}
        JOIN {ref2}
        ON {keys}
        '''.format(**locals())

    elif v.lower().startswith('foreach '):
        ref, fields = re.match("foreach (.*?) generate (.*)", 
                v, flags=re.IGNORECASE).groups()

        if fields.lower().startswith('flatten('):
            f1,f2 = re.match('flatten\(.*?\) as \(\s*(.*?)\s*\),\s*(.*)', fields, flags=re.IGNORECASE).groups()
            fields = '{f1}, {f2}'.format(**locals())

        fields = re.sub(',\s*', ',\n          ', fields).replace('::', '.')

        if recurse: ref = resolve(ref, recurse=recurse)

        return '''
        SELECT {fields}
        FROM {ref}
        '''.format(**locals())

    elif v.lower().startswith('order '):
        ref, ordering, par = re.match('order (.*?) by \(?\s*(.*,\s*[^\s]*)\s*\)?\s*(parallel.*)?',
                v, flags=re.IGNORECASE).groups()

        if recurse: ref = resolve(ref, recurse=recurse)

        return '''
        {ref}
        ORDER BY {ordering}
        '''.format(**locals())

    elif v.lower().startswith('group '):
        ref, grouping, par = re.match('group (.*?) by \(?\s*(.*,\s*[^\s]*)\s*\)?\s*(parallel.*)?',
                v, flags=re.IGNORECASE).groups()

        if recurse: ref = resolve(ref, recurse=recurse)

        return '''
        {ref}
        GROUP BY {grouping}
        '''.format(**locals())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('pig_script', help='path to a pig script to convert to SQL')
    args = parser.parse_args()

    #fnam = 'altgenre_collection_d.pig'
    with open(args.pig_script, 'r') as f:
        lines = f.read()

    no_comments = '\n'.join([
            '{};'.format(ln) if ln.lower().startswith('%default ') else ln  # close define statements with semicolon, since this isn't explicitly necessary in pig.
            for ln in re.sub('/[\*]([\d\D]*?)[\*]/', '', re.sub('--.*?\n', '\n', lines)).split('\n')
            ])
    cleaned_pig = [o for o in re.split('\s*;\s*', re.sub('\s+', ' ', no_comments)) if o != '']
    pig_settings = [o for o in cleaned_pig 
            if o.lower().startswith('set ') 
            or o.lower().startswith('%default ') 
            or o.lower().startswith('register ') 
            or o.lower().startswith('define ')
            ]
    pig_objects = [o for o in cleaned_pig 
            if not o.lower().startswith('set ') 
            and not o.lower().startswith('%default ') 
            and not o.lower().startswith('register ') 
            and not o.lower().startswith('define ')
            ]

    po = {k:v for k,v in [o.split(' = ') for o in pig_objects if ' = ' in o]}

    for ins in [i for i in cleaned_pig if i.lower().startswith('store ')]:
        ref, table = re.match("store (.*?) into '(.*?)'", ins, flags=re.IGNORECASE).groups()

        ref = resolve(ref, recurse=True)
        print 'INSERT OVERWRITE {table} {ref}'.format(**locals())


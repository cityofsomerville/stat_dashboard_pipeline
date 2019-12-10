
import os
import json

ROOT = os.path.dirname(os.path.abspath(__file__))

def clean_r_json():
    with open(os.path.join(ROOT, 'cats_from_R.json'), 'r') as json_file:
        data = json.load(json_file)
    with open(os.path.join(ROOT, 'qscend_cat_key.json'), 'w') as outfile:
        json.dump(data, outfile, indent=4)

def qscend_category_keys():
    with open(os.path.join(ROOT, 'cats_from_R.json'), 'r') as json_file:
        data = json.load(json_file)

    final_dict = {}
    for key, entry in data.items():
        entry.sort()
        for item in entry:
            # Will overwrite with last if duped
            final_dict[item] = key

    with open(os.path.join(ROOT, 'qscend_cat_id_key.json'), 'w') as outfile:
        json.dump(final_dict, outfile, indent=4)



if __name__ == '__main__':
    clean_r_json()
    qscend_category_keys()

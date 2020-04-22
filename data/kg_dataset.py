
import os
import collections
from data.utils import _fetch_dataset

MyDataSet = collections.namedtuple('DatasetMetadata', ['dataset_name', 'filename', 'url', 'train_name', 'valid_name',
                                                 'test_name'])

def fetch_wn18():

    wn18 = MyDataSet(
        dataset_name='wn18',
        filename='wn18.zip',
        url='https://knowledge-graph-data.s3.us-east-2.amazonaws.com/wn18.zip',
        train_name='train.txt',
        valid_name='valid.txt',
        test_name='test.txt',
    )
    fetch_dir = _fetch_dataset(wn18, data_home='./')
    print('{} dataset is fetched in {}'.format(wn18.filename, fetch_dir))

def fetch_wn18rr():

    wn18rr = MyDataSet(
        dataset_name='wn18RR',
        filename='wn18RR.zip',
        url='https://knowledge-graph-data.s3.us-east-2.amazonaws.com/wn18RR.zip',
        train_name='train.txt',
        valid_name='valid.txt',
        test_name='test.txt'
    )
    fetch_dir = _fetch_dataset(wn18rr, data_home='./')
    print('{} dataset is fetched in {}'.format(wn18rr.filename, fetch_dir))

def fetch_fb15k():

    FB15K = MyDataSet(
        dataset_name='fb15k',
        filename='fb15k.zip',
        url='https://knowledge-graph-data.s3.us-east-2.amazonaws.com/fb15k.zip',
        train_name='train.txt',
        valid_name='valid.txt',
        test_name='test.txt'
    )

    fetch_dir = _fetch_dataset(FB15K, data_home='./')
    print('{} dataset is fetched in {}'.format(FB15K.filename, fetch_dir))

def fetch_fb15_237():

    fb15k_237 = MyDataSet(
        dataset_name='fb15k-237',
        filename='fb15k-237.zip',
        url='https://knowledge-graph-data.s3.us-east-2.amazonaws.com/fb15k-237.zip',
        train_name='train.txt',
        valid_name='valid.txt',
        test_name='test.txt'
    )

    fetch_dir = _fetch_dataset(fb15k_237, data_home='./')
    print('{} dataset is fetched in {}'.format(fb15k_237.filename, fetch_dir))

def fetch_yago3_10():

    yago3_10 = MyDataSet(
        dataset_name='YAGO3-10',
        filename='YAGO3-10.zip',
        url='https://knowledge-graph-data.s3.us-east-2.amazonaws.com/YAGO3-10.zip',
        train_name='train.txt',
        valid_name='valid.txt',
        test_name='test.txt'
    )

    fetch_dir = _fetch_dataset(yago3_10, data_home='./')
    print('{} dataset is fetched in {}'.format(yago3_10.filename, fetch_dir))

def fetch_wn11():
    wn11 = MyDataSet(
        dataset_name='wordnet11',
        filename='wordnet11.zip',
        url='https://knowledge-graph-data.s3.us-east-2.amazonaws.com/wordnet11.zip',
        train_name='train.txt',
        valid_name='dev.txt',
        test_name='test.txt'
    )

    fetch_dir = _fetch_dataset(wn11, data_home='./')
    print('{} dataset is fetched in {}'.format(wn11.filename, fetch_dir))

def fetch_fb13():
    fb13 = MyDataSet(
        dataset_name='freebase13',
        filename='freebase13.zip',
        url='https://knowledge-graph-data.s3.us-east-2.amazonaws.com/freebase13.zip',
        train_name='train.txt',
        valid_name='dev.txt',
        test_name='test.txt'
    )
    fetch_dir = _fetch_dataset(fb13, data_home='./')
    print('{} dataset is fetched in {}'.format(fb13.filename, fetch_dir))

def fetch_yago15k():
    yago15k= MyDataSet(
        dataset_name='yago15k',
        filename='yago15k.zip',
        url='https://knowledge-graph-data.s3.us-east-2.amazonaws.com/yago15k.zip',
        train_name='yago15k_train.txt',
        valid_name='yago15k_valid.txt',
        test_name='yago15k_test.txt'
    )
    fetch_dir = _fetch_dataset(yago15k, data_home='./')
    print('{} dataset is fetched in {}'.format(yago15k.filename, fetch_dir))

def fetch_wikidata():
    wiki = MyDataSet(
        dataset_name='wikidata',
        filename='wikidata.zip',
        url='https://knowledge-graph-data.s3.us-east-2.amazonaws.com/wikidata.zip',
        train_name='wiki_train.txt',
        valid_name='wiki_valid.txt',
        test_name='wiki_test.txt'
    )
    fetch_dir = _fetch_dataset(wiki, data_home='./')
    print('{} dataset is fetched in {}'.format(wiki.filename, fetch_dir))

def fetch_gdelt():
    gdelt= MyDataSet(
        dataset_name='gdelt',
        filename='gdelt.zip',
        url='https://knowledge-graph-data.s3.us-east-2.amazonaws.com/gdelt.zip',
        train_name='train.txt',
        valid_name='valid.txt',
        test_name='test.txt'
    )
    fetch_dir = _fetch_dataset(gdelt, data_home='./')
    print('{} dataset is fetched in {}'.format(gdelt.filename, fetch_dir))

def fetch_icews14():
    icews14= MyDataSet(
        dataset_name='icews14',
        filename='icews14.zip',
        url='https://knowledge-graph-data.s3.us-east-2.amazonaws.com/icews14.zip',
        train_name='train.txt',
        valid_name='valid.txt',
        test_name='test.txt'
    )
    fetch_dir = _fetch_dataset(icews14, data_home='./')
    print('{} dataset is fetched in {}'.format(icews14.filename, fetch_dir))

def fetch_icews15():
    icews15= MyDataSet(
        dataset_name='icews05-15',
        filename='icews05-15.zip',
        url='https://knowledge-graph-data.s3.us-east-2.amazonaws.com/icews05-15.zip',
        train_name='train.txt',
        valid_name='valid.txt',
        test_name='test.txt'
    )
    fetch_dir = _fetch_dataset(icews15, data_home='./')
    print('{} dataset is fetched in {}'.format(icews15.filename, fetch_dir))

def fetch_ge19sm():
    ge19sm = MyDataSet(
        dataset_name='ge19sm',
        filename='ge19sm.zip',
        url='https://knowledge-graph-data.s3.us-east-2.amazonaws.com/ge19sm.zip',
        train_name='train.txt',
        valid_name='valid.txt',
        test_name='test.txt'
    )
    fetch_dir = _fetch_dataset(ge19sm, data_home='./')
    print('{} dataset is fetched in {}'.format(ge19sm.filename, fetch_dir))

def fetch_ge19dm():
    ge19dm = MyDataSet(
        dataset_name='ge19dm',
        filename='ge19dm.zip',
        url='https://knowledge-graph-data.s3.us-east-2.amazonaws.com/ge19dm.zip',
        train_name='train.txt',
        valid_name='valid.txt',
        test_name='test.txt'
    )
    fetch_dir = _fetch_dataset(ge19dm, data_home='./')
    print('{} dataset is fetched in {}'.format(ge19dm.filename, fetch_dir))

def fetch_all():
    fetch_wn18()
    fetch_wn18rr()
    fetch_fb15k()
    fetch_fb15_237()
    fetch_yago3_10()
    fetch_wn11()
    fetch_fb13()
    fetch_yago15k()
    fetch_wikidata()
    fetch_gdelt()
    fetch_icews14()
    fetch_icews15()
    fetch_ge19dm()
    fetch_ge19sm()

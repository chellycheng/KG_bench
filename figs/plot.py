import collections
import glob
import os
import datetime
from threading import Lock, Thread
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

MyDataSet = collections.namedtuple('DatasetMetadata', ['dataset_name', 'filename','type',
                                                       'order','axis'])
#  TODO:fb15k,fb15_237 cannot display properly, so set it to not supported
fb15k = MyDataSet(
    dataset_name='fb15k',
    filename='all.txt',
    type = 's',
    order ='hrt',
    axis = 'c'
)
fb15_237 = MyDataSet(
    dataset_name='fb15k-237',
    filename='all.txt',
    type = 's',
    order ='hrt',
    axis = 'c'
)
fb13 = MyDataSet(
    dataset_name='freebase13',
    filename='all.txt',
    type = 's',
    order ='hrty',
    axis = 'r'
)
gdelt = MyDataSet(
    dataset_name='gdelt',
    filename='all.txt',
    type = 'd',
    order ='hrtd',
    axis = 'r'
)
icews15 = MyDataSet(
    dataset_name='icews05-15',
    filename='all.txt',
    type = 'd',
    order ='hrtd',
    axis = 'r'
)

icews14 = MyDataSet(
    dataset_name='icews14',
    filename='all.txt',
    type = 'd',
    order ='hrtd',
    axis = 'r'
)

wikidata = MyDataSet(
    dataset_name='wikidata',
    filename='all.txt',
    type = 'd',
    order ='hrtxd',
    axis = 'c'
)

wn18 = MyDataSet(
    dataset_name='wn18',
    filename='all.txt',
    type = 's',
    order ='hrt',
    axis = 'r'
)

wn18RR = MyDataSet(
    dataset_name='wn18RR',
    filename='all.txt',
    type = 's',
    order ='hrt',
    axis = 'r'
)

wordnet11 = MyDataSet(
    dataset_name='wordnet11',
    filename='all.txt',
    type = 's',
    order ='hrty',
    axis = 'r'
)

yago3 = MyDataSet(
    dataset_name='YAGO3-10',
    filename='all.txt',
    type = 's',
    order ='hrt',
    axis = 'r'
)

yago15 = MyDataSet(
    dataset_name='yago15k',
    filename='all.txt',
    type = 'd',
    order ='hrtxd',
    axis = 'r'
)

ge19sm = MyDataSet(
    dataset_name='ge19sm',
    filename='all.txt',
    type = 's',
    order ='hrt',
    axis = 'r'
)

ge19dm = MyDataSet(
    dataset_name='ge19dm',
    filename='all.txt',
    type = 'd',
    order ='hrtd',
    axis = 'r'
)

list_available = [fb13,gdelt,wikidata,wn18,wn18RR,wordnet11,yago3,ge19sm,ge19dm]
dicitonary = {x.dataset_name: x for x in list_available}

def _s_plotExtract(g,lk,trd_cnt,prt,order):
    st_tm = datetime.datetime.strptime(os.getenv('ST_TM', ''), '%Y-%m-%d-%H')
    sp_tm = datetime.datetime.strptime(os.getenv('SP_TM', ''), '%Y-%m-%d-%H')
    # TODO: have to set global setting
    for fn in glob.glob('../data/graph/*.txt'):
        fn_tm = datetime.datetime.strptime(fn, '../data/graph/%Y-%m-%d-%H.txt')
        if fn_tm < st_tm or fn_tm > sp_tm or fn_tm.timetuple().tm_yday % trd_cnt != prt:
            continue
        df = pd.read_csv(fn, index_col=None,sep="\t",header = 0)
        if order =='htr':
            df.columns  = ['head','tail','relation']
        else:
            df.columns = ['head', 'relation', 'tail']
        with lk:
            g.append(df)

def _d_plotExtract(g,lk,trd_cnt,prt,order):
    st_tm = datetime.datetime.strptime(os.getenv('ST_TM', ''), '%Y-%m-%d-%H')
    sp_tm = datetime.datetime.strptime(os.getenv('SP_TM', ''), '%Y-%m-%d-%H')
    for fn in glob.glob('../data/graph/*.txt'):
        fn_tm = datetime.datetime.strptime(fn, '../data/graph/%Y-%m-%d-%H.txt')
        if fn_tm < st_tm or fn_tm > sp_tm or fn_tm.timetuple().tm_yday % trd_cnt != prt:
            continue
        df = pd.read_csv(fn, index_col=None,sep="\t",header = 0)
        if order =='htr':
            df.columns = ['head', 'tail', 'relation', 'time']
        else:
            df.columns  = ['head','relation','tail','time']

        with lk:
            g.append(df)

def plotExtract(dynamic, order):
    g = []
    lk = Lock()
    trd_cnt = int(os.getenv('TRD_CNT', '16'))
    ts = []
    for i in range(0, trd_cnt):
        if dynamic:
            t = Thread(target=_d_plotExtract, args=(g, lk, trd_cnt, i,order))
        else:
            t = Thread(target=_s_plotExtract, args=(g, lk, trd_cnt, i,order))
        t.start()
        ts.append(t)
    for t in ts:
        t.join()
    frame = pd.concat(g, axis=0, ignore_index=True, names=['head','tail','relation','time'],sort=False)
    return frame

def hist_relation(dataset,data, output_home_path):


    if dataset.axis == 'r':
        data = data.sort_values('relation', ascending=False)
        g_hist = sns.catplot(x='relation', kind="count",
                             data=data)
        output_path = output_home_path + dataset.dataset_name
        [plt.setp(ax.get_xticklabels(), rotation=90) for ax in g_hist.axes.flat]
    else:

        data['relation'] = data['relation'].astype('category')
        # cat_columns = data.select_dtypes(['category']).columns
        # data[cat_columns] = data[cat_columns].apply(lambda x: x.cat.codes)
        data = data.sort_values('relation', ascending=False)
        g_hist = sns.catplot(x='relation', kind="count",
                             data=data)
        output_path = output_home_path + dataset.dataset_name
        [plt.setp(ax.get_xticklabels(), rotation=90,fontsize=5) for ax in g_hist.axes.flat]
            # if i % 50 == 0:
            #     plt.setp(ax.get_xticklabels(), visible=True)
            # else:
            #     plt.setp(ax.get_xticklabels(), visible=False)
            # i+=1
        # i = 25
        # for ax in g_hist.axes.flat:
        #     if i % 50 ==0:
        #         # plt.setp(ax.get_xticklabels(), rotation=90,fontsize=0,horizontalalignment='right')\
        #         plt.setp(ax.get_xticklabels())
        #
        #     else:
        #         plt.setp(ax.get_xticklabels(),visible = False)
        #     i+=1


    plt.savefig(output_path, bbox_inches='tight',tdi = 30000000)

def _plot_static(dataset,home_data_path, output_home_path):
    input_path = home_data_path+dataset.dataset_name+ '/'+dataset.filename
    df = pd.read_csv(input_path, index_col=None, sep="\t", header=0)
    if dataset.order == 'htr':
        df.columns = ['head', 'tail', 'relation']
    elif dataset.order =='hrt':
        df.columns = ['head', 'relation', 'tail']
    else:
        df.columns = ['head', 'relation', 'tail','y']
    hist_relation(dataset, df, output_home_path)

def _plot_dynamic(dataset,home_data_path, output_home_path):
    input_path = home_data_path + dataset.dataset_name + '/' + dataset.filename
    df = pd.read_csv(input_path, index_col=None, sep="\t", header=0)
    if dataset.order == 'htrd':
        df.columns = ['head', 'tail', 'relation','time']
    elif dataset.order == 'hrtd':
        df.columns = ['head', 'relation', 'tail','time']
    else:
        df.columns = ['head', 'relation', 'tail', 'x','time']
    hist_relation(dataset, df, output_home_path)

def plot(filename, home_data_path='../data/', output_home_path='./'):
    try:
        dataset = dicitonary[filename]
    except:
        print("Such dataset does not exist or not support")
        return
    if dataset.filename== 'GE19':
        if dataset.type =='s':
            hist_relation(plotExtract(False, dataset.order))
        else:
            hist_relation(plotExtract(True, dataset.order))
    else:
        if dataset.type =='s':
            _plot_static(dataset,home_data_path, output_home_path)
        else:
            _plot_dynamic(dataset,home_data_path,output_home_path)

def plot_all(home_data_path='../data/', output_path='./'):
    keys = dicitonary.keys()
    for k in keys:
        plot(k,home_data_path,output_path)

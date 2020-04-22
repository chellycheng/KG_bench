from threading import Lock, Thread
import datetime
import re
import collections
import glob
import os

MyDataSet = collections.namedtuple('DatasetMetadata', ['dataset_name', 'filename','type',
                                                       'order','timestamp','nodetype'])
fb15k = MyDataSet(
    dataset_name='fb15k',
    filename='all.txt',
    type = 's',
    order ='hrt',
    timestamp = 'none',
    nodetype = 'na'
)
fb15_237 = MyDataSet(
    dataset_name='fb15k-237',
    filename='all.txt',
    type = 's',
    order ='hrt',
    timestamp = 'none',
    nodetype = 'na'
)
fb13 = MyDataSet(
    dataset_name='freebase13',
    filename='all.txt',
    type = 's',
    order ='hrty',
    timestamp = 'none',
    nodetype = 'na'
)
gdelt = MyDataSet(
    dataset_name='gdelt',
    filename='all.txt',
    type = 'd',
    order ='hrtd',
    timestamp = 'date',
    nodetype = 'na'
)
icews15 = MyDataSet(
    dataset_name='icews05-15',
    filename='all.txt',
    type = 'd',
    order ='hrtd',
    timestamp = 'date',
    nodetype = 'na'
)

icews14 = MyDataSet(
    dataset_name='icews14',
    filename='all.txt',
    type = 'd',
    order ='hrtd',
    timestamp = 'date',
    nodetype = 'na'
)

wikidata = MyDataSet(
    dataset_name='wikidata',
    filename='all.txt',
    type = 'd',
    order ='hrtxd',
    timestamp = 'year',
    nodetype = 'na'
)

wn18 = MyDataSet(
    dataset_name='wn18',
    filename='all.txt',
    type = 's',
    order ='hrt',
    timestamp = 'none',
    nodetype = 'na'
)

wn18RR = MyDataSet(
    dataset_name='wn18RR',
    filename='all.txt',
    type = 's',
    order ='hrt',
    timestamp = 'none',
    nodetype = 'na'
)

wordnet11 = MyDataSet(
    dataset_name='wordnet11',
    filename='all.txt',
    type = 's',
    order ='hrty',
    timestamp = 'none',
    nodetype = 'na'
)

yago3 = MyDataSet(
    dataset_name='YAGO3-10',
    filename='all.txt',
    type = 's',
    order ='hrt',
    timestamp = 'none',
    nodetype = 'na'
)

yago15 = MyDataSet(
    dataset_name='yago15k',
    filename='all.txt',
    type = 'd',
    order ='hrtxd',
    timestamp = 'all',
    nodetype = 'na'
)

ge19sm = MyDataSet(
    dataset_name='ge19sm',
    filename='all.txt',
    type = 's',
    order ='hrt',
    timestamp = 'int',
    nodetype = 'a'
)

ge19dm = MyDataSet(
    dataset_name='ge19dm',
    filename='all.txt',
    type = 'd',
    order ='hrtd',
    timestamp = 'int',
    nodetype = 'a'
)

list_available = [fb15k,fb15_237,fb13,gdelt,icews15,icews14,wikidata,wn18,wn18RR,wordnet11,yago3,yago15,ge19sm,ge19dm]
dicitonary = {x.dataset_name: x for x in list_available}

def _reg(s):
    r = re.match("\/(.*?)\/", s).groups()[0]
    return r

def _d_extract(order, g,s, ts, node, lk, trd_cnt, prt):
    st_tm = datetime.datetime.strptime(os.getenv('ST_TM', ''), '%Y-%m-%d-%H')
    sp_tm = datetime.datetime.strptime(os.getenv('SP_TM', ''), '%Y-%m-%d-%H')
    for fn in glob.glob('../data/graph/*.txt'):
        fn_tm = datetime.datetime.strptime(fn, '../data/graph/%Y-%m-%d-%H.txt')
        if fn_tm < st_tm or fn_tm > sp_tm or fn_tm.timetuple().tm_yday % trd_cnt != prt:
            continue
        #print(fn)
        with open(fn, 'r') as fr:
            for l in fr.readlines():
                if order == 'hrt':
                    v1, r, v2, t = l.strip().split('\t')
                else:
                    v1, v2, r, t = l.strip().split('\t')
                with lk:
                    node.add(_reg(v1))
                    node.add(_reg(v2))
                with lk:
                    if v1 not in g:
                        g[v1] = []
                    g[v1].append(v2)

                with lk:  # NOTE: To find nodes with the most interactions
                    if v2 not in g:
                        g[v2] = []
                    g[v2].append(v1)
                with lk:
                    date= datetime.datetime.strptime(t[:10], '%Y-%m-%d').strftime('%Y-%m-%d')
                    ts.add(date)
                    s.add(r)

def _s_extract(order,g,s, node, lk, trd_cnt, prt):
    st_tm = datetime.datetime.strptime(os.getenv('ST_TM', ''), '%Y-%m-%d-%H')
    sp_tm = datetime.datetime.strptime(os.getenv('SP_TM', ''), '%Y-%m-%d-%H')
    for fn in glob.glob('../data/graph/*.txt'):
        fn_tm = datetime.datetime.strptime(fn, '../data/graph/%Y-%m-%d-%H.txt')
        if fn_tm < st_tm or fn_tm > sp_tm or fn_tm.timetuple().tm_yday % trd_cnt != prt:
            continue
        #print(fn)
        with open(fn, 'r') as fr:
            for l in fr.readlines():
                if order == 'hrt':
                    v1, r, v2  = l.strip().split('\t')
                else:
                    v1, v2, r = l.strip().split('\t')
                with lk:
                    node.add(_reg(v1))
                    node.add(_reg(v2))
                with lk:
                    if v1 not in g:
                        g[v1] = []
                    g[v1].append(v2)

                with lk:  # NOTE: To find nodes with the most interactions
                    if v2 not in g:
                        g[v2] = []
                    g[v2].append(v1)
                with lk:
                    s.add(r)

def extract(dynamic, order):
    g = {}
    s = set()
    tset = set()
    node = set()
    lk = Lock()
    trd_cnt = int(os.getenv('TRD_CNT', '16'))
    ts = []
    for i in range(0, trd_cnt):
        if dynamic:
            t = Thread(target=_d_extract, args=(order,g, s,tset,node,lk, trd_cnt, i))
        else:
            t = Thread(target=_s_extract, args=(order,g, s, node, lk, trd_cnt, i))
        t.start()
        ts.append(t)
    for t in ts:
        t.join()
    return g,s,tset,node

def fetch_statistic(filename, home_data_path='../data/', output =False,output_home_path='./'):
    try:
        dataset = dicitonary[filename]
    except:
        print("Such dataset does not exist or not support")
        return
    if dataset.filename== 'GE19':
        if dataset.type =='s':
            g, s, tset, node = extract(False,dataset.order)
        else:
            g, s, tset, node = extract(True,dataset.order)
        result = _statistics(g)
        if output:
            output_path = output_home_path+filename+".txt"
            with open(output_path, "a") as f:
                print(
                    'Dataset: {} Nodes: {} Edges:{} Max_D:{} Min_D:{} Avg_D:{} Mid_D:{} Relation{} TimeStamp: {} NodeType: {}'.format
                    (dataset.dataset_name, *result, len(list(s)), len(list(tset)), len(list(node))),file = f)
        else:
            print('Dataset: {} Nodes: {} Edges:{} Max_D:{} Min_D:{} Avg_D:{} Mid_D:{} Relation{} TimeStamp: {} NodeType: {}'.format
              (dataset.dataset_name, *result, len(list(s)), len(list(tset)), len(list(node))))
    else:
        if dataset.type =='s':
            _stats_static(dataset,home_data_path, output,output_home_path)
        else:
            _stats_dynamic(dataset,home_data_path, output,output_home_path)

def _stats_static(dataset,home_path,output=False, output_home_path='./'):
    g = {}
    node = set()
    s = set()
    path = home_path+dataset.dataset_name+"/"+dataset.filename
    with open(path, 'r') as fr:
        for l in fr.readlines():
            if dataset.order== 'hrt':
                v1, r, v2 = l.strip().split('\t')
            elif dataset.order =='htr':
                v1, v2, r = l.strip().split('\t')
            else:
                try:
                    v1, r, v2, y = l.strip().split('\t')
                except:
                    v1, r, v2 = l.strip().split('\t')
            if dataset.nodetype != 'na':
                node.add(v1)
                node.add(v2)
            if v1 not in g:
                g[v1] = []
            g[v1].append(v2)

            if v2 not in g:
                g[v2] = []
            g[v2].append(v1)
            s.add(r)
    result = _statistics(g)
    print('Dataset: {} Nodes: {} Edges:{} Max_D:{} Min_D:{} Avg_D:{}'
          ' Mid_D:{} Relation{} TimeStamp: 0 NodeType: {} '.format(dataset.dataset_name,*result,len(list(s)),len(list(node))))
    if output:
        output_path = output_home_path + dataset.dataset_name + ".txt"
        with open(output_path, "a") as f:
            print('Dataset: {} Nodes: {} Edges:{} Max_D:{} Min_D:{} Avg_D:{}'
                  ' Mid_D:{} Relation{} TimeStamp: 0 NodeType: {} '.format(dataset.dataset_name, *result, len(list(s)),
                                                                           len(list(node))),file=f)
    else:
        print('Dataset: {} Nodes: {} Edges:{} Max_D:{} Min_D:{} Avg_D:{}'
              ' Mid_D:{} Relation{} TimeStamp: 0 NodeType: {} '.format(dataset.dataset_name, *result, len(list(s)),
                                                                       len(list(node))))

def _statistics(g):
    # g is a big dictionary
    node = 0
    edge = 0
    max_d = 0
    min_d = 1
    d = {}
    for key, value in g.items():
        node += 1
        edge_c = len(value)
        edge += edge_c
        if edge_c not in d:
            d[edge_c] = 0
        d[edge_c] = d[edge_c] + 1
        max_d = max(max_d, edge_c)
        min_d = min(min_d, edge_c)
    edge /= 2
    avg_d = edge / node

    mid = 0
    mid1 = -1
    if node % 2 == 0:
        temp1 = node / 2
        temp2 = (node / 2) + 1
        #     even number
        for key, value in sorted(d.items()):
            temp1 -= value
            temp2 -= value
            if temp1 <= 0 and mid1 < 0:
                mid1 = key
            if temp2 <= 0:
                mid = (mid1 + key) / 2
                break
    else:
        temp = (node - 1) / 2
        #     odd number (node-1)/2
        for key, value in d.items():
            temp -= value
            if temp <= 0:
                mid = key
                break
    return node, edge, max_d, min_d, avg_d, mid

def _stats_dynamic(dataset,home_path,output=False, output_home_path='./'):
    g = {}
    s = set()
    node = set()
    tset = set()
    path = home_path + dataset.dataset_name + "/" + dataset.filename
    with open(path, 'r') as fr:
        for l in fr.readlines():
            try:
                if dataset.order == 'hrtd':
                    v1, r, v2, t = l.strip().split('\t')
                elif dataset.order == 'htrd':
                    v1, v2, r, t = l.strip().split('\t')
                else:
                    try:
                        v1, r, v2, x, t = l.strip().split('\t')
                    except:
                        v1, r, v2 = l.strip().split('\t')
                        raise ValueError("no time")
                if dataset.timestamp == 'int':
                    date = datetime.datetime.fromtimestamp(int(t)).strftime('%Y-%m-%d')
                    tset.add(date)
                elif dataset.timestamp =='date':
                    date = datetime.datetime.strptime(t, '%Y-%m-%d').strftime('%Y-%m-%d')
                    tset.add(date)
                elif dataset.timestamp =='year':
                    date = datetime.datetime.strptime(t, '%Y').strftime('%Y')
                    tset.add(date)
                else:
                    try:
                        date = datetime.datetime.strptime(t, '\"%Y-##-##\"').strftime('%Y')
                        tset.add(date)
                    except:
                        try:
                            date = datetime.datetime.strptime(t, '\"%Y-%m-%d\"').strftime('%Y')
                            tset.add(date)
                        except:
                            date = datetime.datetime.strptime(t, '\"%Y-%m-##\"').strftime('%Y')
                            tset.add(date)
            except ValueError:
                pass
            finally:
                if v1 not in g:
                    g[v1] = []
                g[v1].append(v2)

                if v2 not in g:
                    g[v2] = []
                g[v2].append(v1)
                s.add(r)

    result = _statistics(g)
    if output:
        output_path = output_home_path + dataset.dataset_name + ".txt"
        with open(output_path, "a") as f:
            print(
                'Dataset: {} Nodes: {} Edges:{} Max_D:{} Min_D:{} Avg_D:{} Mid_D:{} Relation{} TimeStamp: {} NodeType: {}'.format
                (dataset.dataset_name, *result, len(list(s)), len(list(tset)), len(list(node))),file=f)
    else:
        print(
            'Dataset: {} Nodes: {} Edges:{} Max_D:{} Min_D:{} Avg_D:{} Mid_D:{} Relation{} TimeStamp: {} NodeType: {}'.format
            (dataset.dataset_name, *result, len(list(s)), len(list(tset)), len(list(node))))


def fetch_statistic_all(home_data_path='../data/', output =False,output_path='./'):
    keys = dicitonary.keys()
    for k in keys:
        fetch_statistic(k,home_data_path,output,output_path)

fetch_statistic_all(output =True)
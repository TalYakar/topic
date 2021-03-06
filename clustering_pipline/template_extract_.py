# -*- coding: utf-8 -*-
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

import pandas as pd
import cPickle, re
import numpy as np
from collections import Counter
params, temp_counter = [], 0


def sensitiveCosine2(l1, l2):
    l1, l2 = l1.split(), l2.split()
    # l1, l2 = re.findall(r"[\w']+", l1), re.findall(r"[\w']+", l2)
    s = np.sum([1 if x == y else 0 for x, y in zip(l1, l2)])
    return s / float(np.sqrt(len(l1) * (len(l2))))


def param(x):
    global params
    global temp_counter
    params.append(x)
    temp_counter+=1
    return '<<' + x + '>>'


def prob(l,j):
    return [float(l_)/j for l_ in l]


# cl = cPickle.load(open( "data\clusters\clusters_build_AVP-nBVNePvB1HupEh-a_.p", "rb" ))
def template(cl):
    # type: () -> object
    # cl = cPickle.load(open( "clusters_2_.p", "rb" ))
    df_ = pd.DataFrame(columns=['cluster','template', 'cl_length', 'params'])
    df_params = pd.DataFrame(columns=['template', 'dict_length', 'params'])
    [batch, batch_, outcluster, templates, temp] = [], [], [], [], []
    for i, cluster in enumerate(cl):
        print
        print  '---------------cluster' + str(i) +'_length_' + str(len(cluster))+'------------------------\n'
        # batch_ = re.findall(r"[\w']+", cluster.logs[0])
        batch = np.array(cluster.logs[0].split())
        print cluster.logs[0].encode('utf-8')
        params, structure = [], {}
        for j, x_ in enumerate(cluster.logs[1:]):
            print x_.encode('utf-8')
            dist = sensitiveCosine2(cluster.logs[0], x_)
            if dist >= 0.2:
                # temp = []
                temp = x_.split()
                if len(temp) >= 1 and len(temp) == len(cluster.logs[0].split()):
                    batch = np.vstack((batch, temp))
                else:
                    outcluster.append(x_)
        thr2 = round(0.85 * len(cluster))
        if batch != []:
            template, p = [], 0
            for j, x in enumerate(batch.T):
                w_dict = Counter(x)
                # temp = [k if v >= thr2 else '<p>' for k, v in w_dict.iteritems() ]
                if len(w_dict) > 1:
                    params.append(w_dict)
                    template.append('<p_'+str(len(w_dict))+'>')
                    df_params.loc[str(i) + '_' + str(j)+'_' + str(p)] = pd.Series(
                        dict( template=' '.join(template), dict_length=len(w_dict), params=w_dict))
                    p =+ 1
                else:
                    template.append(max(w_dict, key=w_dict.get))
        print
        print  '---------------template' + str(i) + '_length_' + str(len(template)) + '------------------------\n'
        print (' '.join(template)).encode('utf-8')
        df_.loc[str(i)] = pd.Series(dict(cluster=cluster.logs, template=' '.join(template), cl_length=len(cluster), params = params))

    if outcluster!=[]:
        cPickle.dump(cl, open("results/time/outcluster.p", "wb"))
    return cl, df_, df_params


def temp_params(df_params):
    #adding stat
    df_params['sum'] = [sum(df_params.params.ix[i].values()) for i in range(0, len(df_params))]
    df_params['prob'] = [df_params.params.ix[i].values() for i in range(0, len(df_params))]
    df_params['prob'] =[prob(l,j) for l, j in zip(df_params.prob, df_params['sum'])]
    df_params['diversity'] = [list(set(x.values())) for x in df_params.params]
    df_params['temp_len'] = [len(x.split()) for x in df_params.template]
    df_params.to_pickle('results/df_failed_clusters_params_{0}.pkl'.format(len(df_params)))
    return df_params


    #outcluster
    for i, cluster in enumerate(outcluster):
        print  '---------------outcluster' +str(i)+'_length_' + str(len(outcluster))+'------------------------\n'
        for x_ in outcluster:
            print x_.encode('utf-8')
    return df_params

def main():
    cl = cPickle.load(open("clusters_failed_16.p", "rb"))
    cl, df_, df_params = template(cl)
    df_params = temp_params(df_params)
    cPickle.dump(cl, open("results/templates.p", "wb"))
    df_.to_pickle('results/failed_clusters_templates_{0}.pkl'.format(len(cl)))

if __name__ == '__main__':
    print main()

import numpy as np
"""
    common evaluation method fot the KG brench
"""

def hits_at_n_score(ranks, n):

    if isinstance(ranks, list):
        ranks = np.asarray(ranks)
    ranks = ranks.reshape(-1)
    return np.sum(ranks <= n) / len(ranks)

def mrr_score(ranks):

    if isinstance(ranks, list):
        ranks = np.asarray(ranks)
    ranks = ranks.reshape(-1)
    return np.sum(1 / ranks) / len(ranks)

def rank_score(y_true, y_pred, pos_lab=1):

    idx = np.argsort(y_pred)[::-1]
    y_ord = y_true[idx]
    rank = np.where(y_ord == pos_lab)[0][0] + 1
    return rank

def mr_score(ranks):
    if isinstance(ranks, list):
        ranks = np.asarray(ranks)
    ranks = ranks.reshape(-1)
    return np.sum(ranks) / len(ranks)

def evaluation(ranks):
    mrr = mrr_score(ranks)
    mr = mr_score(ranks)
    hits_1 = hits_at_n_score(ranks, n=1)
    hits_3 = hits_at_n_score(ranks, n=3)
    hits_10 = hits_at_n_score(ranks, n=10)
    return mrr, mr, hits_1, hits_3, hits_10

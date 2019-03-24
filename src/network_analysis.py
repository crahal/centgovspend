import pandas as pd
import itertools
from tqdm import tqdm


def avgFriendDegree(v, G):
    """ Calculate the average degree of the neighbors of a node"""
    degSum = 0
    for u in G.neighbors(v):
        degSum += G.degree(u)
    return degSum / G.degree(v)


def add_datetimes(df):
    tqdm.pandas(desc="Converting 'Appointed' strings to datetime!")
    tempdate = df[df['Appointed'].notnull()]
    tempdate = tempdate.drop_duplicates(subset='Appointed', keep='first')
    tempdate['Appointed_DT'] = tempdate['Appointed'].\
        progress_apply(pd.to_datetime,
                       dayfirst=True,
                       errors='coerce')
    df = pd.merge(df,
                  tempdate[['Appointed', 'Appointed_DT']],
                  how='left', left_on='Appointed',
                  right_on='Appointed')
    tqdm.pandas(desc="Converting 'Resigned' strings to datetime!")
    tempdate = df[df['Resigned'].notnull()]
    tempdate = tempdate.drop_duplicates(subset='Resigned', keep='first')
    tempdate['Resigned_DT'] = tempdate['Resigned'].\
        progress_apply(pd.to_datetime,
                       dayfirst=True,
                       errors='coerce')
    df = pd.merge(df,
                  tempdate[['Resigned',
                            'Resigned_DT']],
                  how='left',
                  left_on='Resigned',
                  right_on='Resigned')
    return df


def make_edgelist(ch_off, time_active, edgepath, seper=';'):
    ch_off = ch_off[ch_off['New_ID'] != 'N\\A']
    ch_off = ch_off[(time_active >
                     ch_off['Appointed_DT'].dt.year) &
                    ((time_active <=
                      ch_off['Resigned_DT'].dt.year) |
                     (ch_off['Resigned_DT'].dt.year).isnull())]
    only_essentials = ch_off[['CompanyNumber', 'New_ID']]
    edge_df = pd.DataFrame(only_essentials.
                           groupby('New_ID')['CompanyNumber'].
                           apply(lambda x: list(itertools.combinations(x,
                                                                       2))))
    edge_df[edge_df['CompanyNumber'].apply(len) > 0]
    officer_set = set(edge_df.index)
    set_of_edges = set()
    company_set = set()
    for toople in tqdm(edge_df['CompanyNumber'].tolist()):
        for pair in toople:
            company_set.add(pair[0])
            company_set.add(pair[1])
            if (pair[0] + ';' + pair[1] not in set_of_edges) and\
               (pair[1] + ';' + pair[0] not in set_of_edges) and\
               (pair[1] != pair[0]):
                set_of_edges.add(pair[0] + seper + pair[1])
    with open(edgepath, 'w') as f:
        for item in tqdm(list(set_of_edges)):
            f.write("%s\n" % item)
    return set_of_edges, company_set, officer_set

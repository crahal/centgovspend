from tqdm import tqdm
import requests
import pandas as pd
import numpy as np
import statsmodels.formula.api as smf
import gender_guesser.detector as gender
import itertools
import json
gendet = gender.Detector()


def bus_div_reg(ch_reg, off_count, filepath):
    ''' business diversity regression'''
    ch_reg = ch_reg[ch_reg['OfficerCount']>off_count]
    ch_reg = ch_reg[(ch_reg['ROI']<1000) & (ch_reg['ROI']>-1000)]
    results = smf.ols('ROI ~ blau_gender + ' +
                      'blau_age + blau_nationality + C(SIMPLE_SIC) + CompanyAge + ' +
                      'AverageNumberEmployeesDuringPeriod + AgeMean',
                      data=ch_reg).fit(cov_type='HC3')
    f = open(filepath,'w')
    f.write(results.summary().as_csv())
    f.close()
    print(results.summary())
    return results


def millions(x, pos):
    'The two args are the value and tick position'
    return '%1.1fM' % (x * 1e-6)


def make_blau(input_series):
    ''' accepts a string series,
    returns a float between 0 and 1 '''
    blau_float = 1
    for uniq in input_series.unique():
        frac = (len(input_series[input_series==uniq])/len(input_series))
        blau_float = blau_float - (frac**2)
    return blau_float


def make_edgelist(ch_off, time_active, edgepath):
    ch_off = ch_off[ch_off['New_ID'] != 'N\\A']
    ch_off['Officer ID'] = ch_off['New_ID'].str.split('/', expand=True)[2]
    ch_off = ch_off[(time_active >
                     ch_off['Appointed_DT'].dt.year) &
                    ((time_active <=
                      ch_off['Resigned_DT'].dt.year) |
                     (ch_off['Resigned_DT'].dt.year).isnull())]
    ch_off = ch_off[ch_off['Officer Role'].str.lower() == 'director']
    print(pd.merge(ch_off.groupby('New_ID').
                   size().
                   reset_index().rename({0:
                                         'count'},
                                        axis=1).
                   sort_values(by='count',
                               ascending=False).
                   set_index('New_ID'),
                   ch_off[['New_ID', 'Name']].drop_duplicates(),
                   how='left',
                   left_index=True,
                   right_on='New_ID')[0:10].to_string(index=False))
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
                set_of_edges.add(pair[0] + ';' + pair[1])
    with open(edgepath, 'w') as f:
        for item in tqdm(list(set_of_edges)):
            f.write("%s\n" % item)
    return set_of_edges, company_set, officer_set


def send_df_to_clean_for_nat(df, anom_remove, anom_replace):
    uniq_nats = pd.merge(pd.DataFrame(
        df['nationality'].drop_duplicates()),
        pd.DataFrame(
        df['nationality'].drop_duplicates()),
        how='left',
        left_index=True,
        right_index=True)
    tqdm.pandas(desc="Cleaning nationalities...")
    uniq_nats['nationality_y'] = uniq_nats['nationality_y'].\
        progress_apply(lambda x:
                       clean_nationality(x,
                                         anom_remove,
                                         anom_replace))
    df = pd.merge(df, uniq_nats, how='left',
                  left_on='nationality', right_on='nationality_x')
    df = df.rename(columns={"nationality_y": 'nationality_cleaned'})
    return df


def evaluate_nationality_merge(df, un_states):
    bad_names = [i for i in df['nationality_cleaned'].unique()
                 if i not in un_states]
    unseen_states = [
        i for i in un_states if i not in df['nationality_cleaned'].unique()]
    print('\nIn total, we see: ' +
          str(len(df[df['nationality_cleaned'] !=
                     'N\\A']['nationality_cleaned'].unique())) +
          ' unique UN Member States, Overseas Territories or countries' +
          ' as yet unrecognised.\n')
    print('There are a total of ' +
          str(len(un_states)) +
          ' UN Member States and we see ' +
          str(len(un_states) - len(unseen_states)) +
          ' of them.\n')
    holder = 'The UN Member states that we dont observe are: '
    for unseen in unseen_states:
        holder = holder + unseen + ', '
    print(holder[:-2] + '\n')
    holder = 'The non-UN Member states that we do observe are: '
    bad_names.remove('N\\A')
    for bad in bad_names:
        holder = holder + bad + ', '
    print(holder[:-2] +
          '. Might need to add some new looking entries to the list and dict.')


def clean_nationality(nationality,
                      anomolies_to_remove,
                      anomolies_to_replace):
    ways_to_spell_citizen = ['citizen',
                             'citizan',
                             'citen',
                             'citiz',
                             'citize',
                             'citizine',
                             'cityzen',
                             'citzen',
                             'ctizen']
    for ways in ways_to_spell_citizen:
        nationality = str(nationality).lower().replace(ways, '')
    nationality = nationality.lower().replace('.', '')
    nationality = nationality.lower().replace('born in', '')
    nationality = nationality.lower().replace('nationality', '')
    nationality = nationality.lower().replace('national', '')
    nationality = nationality.lower().replace('overseas', '')
    nationality = nationality.lower().replace('territory', '')
    nationality = nationality.lower().replace('kingdom of', '')
    nationality = nationality.lower().replace('resident', '')
    nationality = nationality.lower().replace('federation', '')
    nationality = nationality.lower().replace('the ', '')
    nationality = nationality.lower().replace('subject', '')
    nationality = nationality.lower().replace('by birth', '')
    nationality = nationality.lower().replace('refugee', '')
    nationality = nationality.lower().replace('naturalized', '')
    nationality = nationality.lower().replace('kingdom of', '')
    nationality = nationality.lower().replace('kingdom ', '')
    nationality = nationality.lower().replace('republic of ', '')
    nationality = nationality.lower().replace('rep of ', '')
    nationality = nationality.lower().replace('rebublic ', '')
    nationality = nationality.lower().replace('rebublic of ', '')
    nationality = nationality.lower().replace('republica of ', '')
    nationality = nationality.lower().replace('repulic of ', '')
    nationality = nationality.lower().replace('federal ', '')
    to_remove = ['&', ' and', '(', ')', ',', '/', '-', '#', '\\',
                 '+', '[', ']', '?', 'dual', ';', '*', '`', '@']
    if len(str(nationality)) == 1:
        nationality = 'N\A'
    for remove_substring in to_remove:
        if remove_substring in nationality.lower():
            nationality = 'N\A'
    if any(char.isdigit() for char in nationality):
        nationality = 'N\A'
    for anomolies in anomolies_to_remove:
        if anomolies.lower().strip() == nationality.lower().strip():
            nationality = 'N\A'
            break
    for key, value in anomolies_to_replace.items():
        if key.lower().strip() == nationality.lower().strip():
            nationality = value
            break
    nationality = str(nationality).lower().replace('"""', '')
    nationality = str(nationality).lower().replace('yugoslavia', 'N\A')
    nationality = str(nationality).lower().replace('N\\An', 'N\A')
    if str(nationality.lower().strip()) == 'nan':
        nationality = 'N\A'
    if len(str(nationality.strip())) <= 1:
        nationality = 'N\A'
    if len(str(nationality.strip())) == 2:
        return nationality.upper().strip()
    else:
        return nationality.title().strip()


def clean_officer_names(x):
    try:
        if len(x.split(' ')) >= 1:
            return x.split(' ')[1]
    except AttributeError:
        pass
    except IndexError:
        pass


def call_namsor_api(row):
    baseurl = 'http://api.namsor.com/onomastics/api/json/gendre/'
    surname = row['name'].split(' ')[-1]
    namsor_return = requests.get(baseurl + row['forename'] + '/' + surname)
    return json.loads(namsor_return.text)['gender']


def make_gender(ch_psc, ch_off, ch_basic=None):
    tqdm.pandas(desc="Calculating gender of PSC!")
    ch_psc['title'] = ch_psc['title'].str.replace('[^\w\s]', '')
    ch_psc['forename'] = ch_psc['forename'].str.replace('[^\w\s]', '')
    ch_psc['gender'] = ch_psc.progress_apply(gender_by_row, axis=1)
    if ch_basic is not None:
        ch_psc = pd.merge(ch_psc, ch_basic,
                          how='left',
                          left_on='company_number',
                          right_on=' CompanyNumber')
    tqdm.pandas(desc="Splitting Officer Forenames!")
    ch_off.loc[:, 'forename'] = ch_off['Name'].progress_apply(
        lambda x: clean_officer_names(x))
    ch_off['forename'] = ch_off['forename'].str.replace('[^\w\s]', '')
    tqdm.pandas(desc="Calculating gender of Officers!")
    ch_off['gender'] = ch_off.progress_apply(gender_by_row, axis=1)
    if ch_basic is not None:
        ch_off = pd.merge(ch_off, ch_basic, how='left',
                          left_on='CompanyNumber',
                          right_on=' CompanyNumber')
    return ch_psc, ch_off


def make_age(df, Age_Type, age_at_date):
    ''' unique stops evaluating non-unique dates millions of times '''

    df['date_of_birth'] = np.where(df['date_of_birth'].
                                   astype(str) != 'nan',
                                   df['date_of_birth'],
                                   np.nan)
    df['date_of_birth'] = np.where(df['date_of_birth'].str.
                                   contains('year'),
                                   df['date_of_birth'],
                                   np.nan)
    unique_date = pd.DataFrame(df['date_of_birth'].
                               drop_duplicates(),
                               columns=['date_of_birth'])
    unique_date = unique_date[unique_date['date_of_birth'].notnull()]
    unique_date['Eval Date'] = unique_date['date_of_birth'].apply(
        lambda x: dict(eval(x)))
    unique_date = pd.merge(unique_date['Eval Date'].
                           apply(pd.Series),
                           unique_date,
                           left_index=True,
                           right_index=True)
    unique_date = unique_date[(unique_date['year'] > 1900) &
                              ((unique_date['year'] < 2019))]
    unique_date['Cleaned Date'] = unique_date['year'].map(
        str) + '-' + unique_date['month'].map(str) + '-01'
    unique_date[Age_Type] = (age_at_date -
                             pd.to_datetime(unique_date['Cleaned Date'],
                                            errors='coerce')) / np.timedelta64(1, 'Y')
    unique_date[Age_Type] = np.where((unique_date[Age_Type] >= 0) & (
        unique_date[Age_Type] <= 110), unique_date[Age_Type], np.nan)
    df = pd.merge(df, unique_date[[Age_Type, 'date_of_birth']],
                  left_on='date_of_birth',
                  right_on='date_of_birth',
                  how='left')
    return df


def make_timely_gender(ch_psc, ch_off):
    ''' boolean filters on gender are not contracting the main dataframe '''
    ch_psc = ch_psc[(ch_psc.gender == 'female') | (ch_psc.gender == 'male')]
    ch_psc.loc[:, 'isfemale'] = np.where(ch_psc['gender'] == 'female', 1, 0)
    ch_off = ch_off[(ch_off.gender == 'female') | (ch_off.gender == 'male')]
    ch_off.loc[:, 'isfemale'] = np.where(ch_off['gender'] == 'female', 1, 0)
    timely_df_a_gender = pd.DataFrame(columns=['Female Appointments (%)',
                                               'Female Resignations (%)'])
    ch_off_nonansic = ch_off[~ch_off['SIMPLE_SIC'].isnull()]
    sic_timely_off_gender = pd.DataFrame(index=list(
        ch_off_nonansic['SIMPLE_SIC'].unique()),
        columns=range(1992, 2019))
    for year in tqdm(list(range(1992, 2019))):
        timely_df_a_gender.at[str(year), 'Female Resignations (%)'] = ch_off[
            (ch_off['Resigned_DT'].dt.year == year)]['isfemale'].mean()
        timely_df_a_gender.at[str(year), 'Female Appointments (%)'] = ch_off[
            (ch_off['Appointed_DT'].dt.year == year)]['isfemale'].mean()
        for sic in list(ch_off_nonansic['SIMPLE_SIC'].unique()):
            active = ch_off_nonansic[(year >=
                                      ch_off_nonansic['Appointed_DT'].dt.
                                      year) &
                                     ((year <=
                                       ch_off_nonansic['Resigned_DT'].
                                       dt.year) |
                                      (ch_off_nonansic['Resigned_DT'].dt
                                       .year).isnull())]
            active = active[active['SIMPLE_SIC'] == sic]
            sic_timely_off_gender.at[sic,
                                     year] = active['isfemale'].mean() * 100
    return sic_timely_off_gender, timely_df_a_gender


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


def make_timely_age(ch_psc, ch_off):
    ch_off = ch_off[ch_off['Resigned_Age'].notnull()]
    ch_off = ch_off[ch_off['Appointed_Age'].notnull()]
    timely_df_a_age = pd.DataFrame(columns=['Av. Age of Appointments',
                                            'Av. Age of Resignations'])
    ch_off_nonansic = ch_off[~ch_off['SIMPLE_SIC'].isnull()]
    sic_timely_off_age = pd.DataFrame(index=list(
        ch_off_nonansic['SIMPLE_SIC'].unique()),
        columns=range(1992, 2019))
    for year in tqdm(list(range(1992, 2019))):
        timely_df_a_age.at[str(year), 'Av. Age of Appointments'] = ch_off[
            (ch_off['Resigned_DT'].dt.year == year)]['Appointed_Age'].mean()
        timely_df_a_age.at[str(year), 'Av. Age of Resignations'] = ch_off[
            (ch_off['Appointed_DT'].dt.year == year)]['Resigned_Age'].mean()
        active = ch_off_nonansic[(year >=
                                  ch_off_nonansic['Appointed_DT'].dt.year) &
                                 ((year <=
                                   ch_off_nonansic['Resigned_DT'].dt.year) |
                                  (ch_off_nonansic['Resigned_DT'].dt.year).
                                  isnull())]
        for sic in list(ch_off_nonansic['SIMPLE_SIC'].unique()):
            active_temp = active[active['SIMPLE_SIC'] == sic]
            sic_timely_off_age.at[sic,
                                  year] = active_temp['Appointed_Age'].mean()
    return sic_timely_off_age, timely_df_a_age


def gender_by_row(row):
    gender = None
    if 'title' in row.index:
        if str(row['title']) != 'nan':
            title = row['title'].lower()
            title = title.replace('the', '')
            title = title.replace('.', '')
            title = title.replace(',', '')
            title = title.replace('/', '')
            list_of_female_titles = ['mrs', 'ms', 'miss', 'lady',
                                     'madame', 'madam', 'sister',
                                     'princess', 'countess', 'baroness',
                                     'dame', 'duchess']
            list_of_male_titles = ['mr', 'master', 'sir', 'lord',
                                   'baron', 'monsieur', 'brother',
                                   'prince', 'duke']
            if title in list_of_female_titles:
                gender = 'female'
            elif title in list_of_male_titles:
                gender = 'male'
    if gender is None:
        if str(row['forename']) != 'nan':
            gender = gendet.get_gender(row['forename']).lower()
            gender = gender.replace('mostly_', '').lower()
        else:
            return np.nan
    if (gender == 'female') or (gender == 'male'):
        return gender
    else:
        return np.nan


def drop_dupes(ch_off, ch_psc):
    print('We drop ' +
          str(len(ch_off) - len(ch_off.
                                drop_duplicates(subset=['CompanyNumber',
                                                        'Name',
                                                        'Officer ID',
                                                        'Appointed',
                                                        'Officer Role']))) +
          ' potentially duplicate officers (' +
          str(round(100 -
                    (len(ch_off.drop_duplicates(subset=['CompanyNumber',
                                                        'Name',
                                                        'Officer ID',
                                                        'Appointed',
                                                        'Officer Role'])) /
                     len(ch_off) * 100), 3)) + '%).')
    ch_off = ch_off.drop_duplicates(subset=['CompanyNumber',
                                            'Name',
                                            'Officer ID',
                                            'Appointed',
                                            'Officer Role'])
    print('We drop ' +
          str(len(ch_psc) - len(ch_psc.
                                drop_duplicates(subset=['company_number',
                                                        'postal_code',
                                                        'date_of_birth',
                                                        'name',
                                                        'forename',
                                                        'title',
                                                        'nationality']))) +
          ' potentially duplicate PSC (' +
          str(round(100 -
                    (len(ch_psc.drop_duplicates(subset=['company_number',
                                                        'postal_code',
                                                        'date_of_birth',
                                                        'name',
                                                        'forename',
                                                        'title',
                                                        'nationality'])) /
                     len(ch_psc) * 100), 3)) + '%).')
    ch_psc = ch_psc.drop_duplicates(subset=['company_number',
                                            'postal_code',
                                            'date_of_birth',
                                            'name',
                                            'forename',
                                            'title',
                                            'nationality'])
    return ch_off, ch_psc


def only_active(ch_off, ch_psc, ch_basic):
    print('We drop ' +
          str(len(ch_psc) -
              len(ch_psc[ch_psc['company_number']
                         .isin(list(ch_basic[' CompanyNumber']))])) +
          ' rows of PSC data which is not in the ch_basic flatfile (' +
          str(((len(ch_psc) -
                len(ch_psc[ch_psc['company_number']
                           .isin(list(ch_basic[' CompanyNumber']))])) /
               len(ch_psc) * 100)) + '%).')
    ch_psc = ch_psc[ch_psc['company_number'].isin(
        list(ch_basic[' CompanyNumber']))]
    print('We drop ' +
          str(len(ch_off) -
              len(ch_off[ch_off['CompanyNumber']
                         .isin(list(ch_basic[' CompanyNumber']))])) +
          ' rows of Officer data which is not in the ch_basic flatfile (' +
          str(((len(ch_off) -
                len(ch_off[ch_off['CompanyNumber']
                           .isin(list(ch_basic[' CompanyNumber']))])) /
               len(ch_off) * 100)) +
          '%).')
    ch_off = ch_off[ch_off['CompanyNumber'].isin(
        list(ch_basic[' CompanyNumber']))]
    print('Length of ch_off: ' + str(len(ch_off)) + '.\n' +
          'Length of ch_psc: ' + str(len(ch_psc)) + '.')
    return ch_off, ch_psc


def evaluate_clean_dfs(ch_off, ch_psc, ch_basic, ch_accounts):
    print('This leaves us with a total of ' +
          str(len(ch_off)) +
          ' Officers in our dataset.')
    print('This leaves us with a total of ' +
          str(len(ch_psc)) +
          ' PSC in our dataset.')
    print('This leaves us with a total of ' +
          str(len(ch_off['CompanyNumber'].unique())) +
          ' unique companies in our Officer dataset.')
    print('This leaves us with a total of ' +
          str(len(ch_psc['company_number'].unique())) +
          ' unique companies in our PSC dataset.')
    print('This leaves us with a total of ' +
          str(len(ch_basic[' CompanyNumber'].unique())) +
          ' companies in our ch_basic dataset.')
    print('We estimate an average of ' +
          str(round(len(ch_off) / len(ch_off['CompanyNumber'].unique()), 2)) +
          ' reported Officers per company.')
    print('We estimate an average of ' +
          str(round(len(ch_psc) / len(ch_psc['company_number'].unique()), 2)) +
          ' reported PSC per company.')
    print('We have data on accounts on a total of ' +
          str(len(ch_accounts)) + ' unique companies.')
    print('The current assets of these companies totals: £' +
          str(round(ch_accounts['CurrentAssets'].sum() / 1000000000, 2)))

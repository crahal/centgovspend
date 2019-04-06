import pandas as pd
import statsmodels.api as sm
import statsmodels.formula.api as smf
import numpy as np
from tqdm import tqdm
from ch_load_data import sicmaker

def gen_reg_toplot(ch_reg, cutoff_range=range(0,11)):
    df_gend = pd.DataFrame(index=cutoff_range,
                           columns=['Cur_Ass_Est',
                                    'Cur_Ass_StdErr',
                                    'Net_Cur_Est',
                                    'Net_Cur_StdErr',
                                    'Fix_Ass_Est',
                                    'Fix_Ass_StdErr'])
    df_age = pd.DataFrame(index=cutoff_range,
                          columns=['Cur_Ass_Est',
                                   'Cur_Ass_StdErr',
                                   'Net_Cur_Est',
                                   'Net_Cur_StdErr',
                                   'Fix_Ass_Est',
                                   'Fix_Ass_StdErr'])
    df_nat = pd.DataFrame(index=cutoff_range,
                          columns=['Cur_Ass_Est',
                                   'Cur_Ass_StdErr',
                                   'Net_Cur_Est',
                                   'Net_Cur_StdErr',
                                   'Fix_Ass_Est',
                                   'Fix_Ass_StdErr'])
    for cutoff in cutoff_range:
        tempdata = ch_reg[ch_reg['OfficerCount']>=cutoff]
        tempdata['CurrentAssets'] = tempdata['CurrentAssets']/1000000
        tempdata['NetCurrentAssetsLiabilities'] = tempdata['NetCurrentAssetsLiabilities']/1000000
        tempdata['FixedAssets'] = tempdata['FixedAssets']/1000000
        results = smf.ols('CurrentAssets ~ blau_gender + ' +
                          'blau_age + blau_nationality + ' +
                          'C(SIMPLE_SIC) + CompanyAge + ' +
                          'AverageNumberEmployeesDuringPeriod + AgeMean',
                          data=tempdata).fit()
        df_gend.at[cutoff, 'Cur_Ass_Est'] = results.params['blau_gender']
        df_gend.at[cutoff, 'Cur_Ass_StdErr'] = results.bse['blau_gender']
        df_age.at[cutoff, 'Cur_Ass_Est'] = results.params['blau_age']
        df_age.at[cutoff, 'Cur_Ass_StdErr'] = results.bse['blau_age']
        df_nat.at[cutoff, 'Cur_Ass_Est'] = results.params['blau_nationality']
        df_nat.at[cutoff, 'Cur_Ass_StdErr'] = results.bse['blau_nationality']

        results = smf.ols('NetCurrentAssetsLiabilities ~ blau_gender + ' +
                          'blau_age + blau_nationality + ' +
                          'C(SIMPLE_SIC) + CompanyAge + ' +
                          'AverageNumberEmployeesDuringPeriod + AgeMean',
                          data=tempdata).fit()
        df_gend.at[cutoff, 'Net_Cur_Est'] = results.params['blau_gender']
        df_gend.at[cutoff, 'Net_Cur_StdErr'] = results.bse['blau_gender']
        df_age.at[cutoff, 'Net_Cur_Est'] = results.params['blau_age']
        df_age.at[cutoff, 'Net_Cur_StdErr'] = results.bse['blau_age']
        df_nat.at[cutoff, 'Net_Cur_Est'] = results.params['blau_nationality']
        df_nat.at[cutoff, 'Net_Cur_StdErr'] = results.bse['blau_nationality']

        results = smf.ols('FixedAssets ~ blau_gender + ' +
                          'blau_age + blau_nationality + ' +
                          'C(SIMPLE_SIC) + CompanyAge + ' +
                          'AverageNumberEmployeesDuringPeriod + AgeMean',
                          data=tempdata).fit()
        df_gend.at[cutoff, 'Fix_Ass_Est'] = results.params['blau_gender']
        df_gend.at[cutoff, 'Fix_Ass_StdErr'] = results.bse['blau_gender']
        df_age.at[cutoff, 'Fix_Ass_Est'] = results.params['blau_age']
        df_age.at[cutoff, 'Fix_Ass_StdErr'] = results.bse['blau_age']
        df_nat.at[cutoff, 'Fix_Ass_Est'] = results.params['blau_nationality']
        df_nat.at[cutoff, 'Fix_Ass_StdErr'] = results.bse['blau_nationality']

    return df_gend, df_age, df_nat


def avgFriendDegree(v, Giant):
    """ Calculate the average degree of the neighbors of a node"""
    degSum = 0
    for u in Giant.neighbors(v):
        degSum += Giant.degree(u)
    return degSum / Giant.degree(v)


def gen_net_toplot(Giant, Entire, centrality, component_size_count):
    df_degrees = pd.DataFrame()
    df_degrees['self_degree'] = [Giant.degree(v) for v in Giant.nodes()]
    df_degrees['av_friend_degree'] = [avgFriendDegree(v, Giant) for v in Giant.nodes()]
    sorted_deg_cent_Entire = sorted(centrality.DegreeCentrality(Giant).run().scores(), reverse=True)
    sorted_deg_cent_Giant = sorted(centrality.DegreeCentrality(Giant).run().scores(), reverse=True)
    c_size_toplot = component_size_count.groupby('componentsize')['componentsize'].count()
    return df_degrees, sorted_deg_cent_Giant, sorted_deg_cent_Entire, c_size_toplot


def gen_desc_toplot(ch_off, ch_psc, ch_basic):
    ''' '''
    ch_off_count = pd.DataFrame(ch_off.groupby(['CompanyNumber'])
                                ['CompanyNumber']
                                .count()).rename({'CompanyNumber':
                                                  '# Officers'}, axis=1)
    ch_psc_count = pd.DataFrame(ch_psc.groupby(['company_number'])
                                ['company_number']
                                .count()).rename({'company_number':
                                                  '# PSC'}, axis=1)
    ch_counts = pd.merge(ch_off_count,
                         ch_psc_count,
                         how='outer',
                         left_index=True,
                         right_index=True)
    ch_counts['Officer Size'] = np.nan
    ch_counts['Officer Size'] = np.where(ch_counts['# Officers'] == 1, '1',
                                         ch_counts['Officer Size'])
    ch_counts['Officer Size'] = np.where(ch_counts['# Officers'] == 2, '2',
                                         ch_counts['Officer Size'])
    ch_counts['Officer Size'] = np.where((ch_counts['# Officers'] >= 3) &
                                         (ch_counts['# Officers'] <= 4),
                                         '3-4', ch_counts['Officer Size'])
    ch_counts['Officer Size'] = np.where((ch_counts['# Officers'] >= 5) &
                                         (ch_counts['# Officers'] <= 7),
                                         '5-7', ch_counts['Officer Size'])
    ch_counts['Officer Size'] = np.where((ch_counts['# Officers'] >= 8) &
                                         (ch_counts['# Officers'] <= 14),
                                         '8-14', ch_counts['Officer Size'])
    ch_counts['Officer Size'] = np.where((ch_counts['# Officers'] >= 15) &
                                         (ch_counts['# Officers'] <= 24),
                                         '15-24', ch_counts['Officer Size'])
    ch_counts['Officer Size'] = np.where((ch_counts['# Officers'] >= 25),
                                         '25+', ch_counts['Officer Size'])
    ch_counts['PSC Size'] = np.nan
    ch_counts['PSC Size'] = np.where(ch_counts['# PSC'] == 1, '1',
                                     ch_counts['PSC Size'])
    ch_counts['PSC Size'] = np.where(ch_counts['# PSC'] == 2, '2',
                                     ch_counts['PSC Size'])
    ch_counts['PSC Size'] = np.where((ch_counts['# PSC'] >= 3) &
                                     (ch_counts['# PSC'] <= 4),
                                     '3-4', ch_counts['PSC Size'])
    ch_counts['PSC Size'] = np.where((ch_counts['# PSC'] >= 5) &
                                     (ch_counts['# PSC'] <= 7),
                                     '5-7', ch_counts['PSC Size'])
    ch_counts['PSC Size'] = np.where((ch_counts['# PSC'] >= 8) &
                                     (ch_counts['# PSC'] <= 14),
                                     '8-14', ch_counts['PSC Size'])
    ch_counts['PSC Size'] = np.where((ch_counts['# PSC'] >= 15) &
                                     (ch_counts['# PSC'] <= 24),
                                     '15-24', ch_counts['PSC Size'])
    ch_counts['PSC Size'] = np.where((ch_counts['# PSC'] >= 25),
                                     '25+', ch_counts['PSC Size'])
    ch_counts_toplot = pd.merge(pd.DataFrame(ch_counts['Officer Size']
                                             .value_counts(),
                                             columns=['Officer Size']),
                                pd.DataFrame(ch_counts['PSC Size']
                                             .value_counts(),
                                             columns=['PSC Size']),
                                how='left',
                                left_index=True,
                                right_index=True)
    ch_counts_toplot['Officer Size (%)'] = ch_counts_toplot[ch_counts_toplot.index!='nan']['Officer Size']/ch_counts_toplot[ch_counts_toplot.index!='nan']['Officer Size'].sum()
    ch_counts_toplot['PSC Size (%)'] = ch_counts_toplot[ch_counts_toplot.index!='nan']['PSC Size']/ch_counts_toplot[ch_counts_toplot.index!='nan']['PSC Size'].sum()
    startyear = 1970
    endyear = 2018
    ch_basic['SIC_INT'] = ch_basic['SICCode.SicText_1']\
        .str.extract('(\d+)')
    ch_basic['SIC_INT'] = pd.to_numeric(ch_basic['SIC_INT'])
    ch_basic['SIMPLE_SIC'] = ch_basic['SIC_INT'].map(lambda x:
                                                     sicmaker(x))
    ch_basic['IncorporationYear'] = ch_basic['IncorporationDate']\
        .str.split('/', expand=True)[2]
    ch_short = ch_basic[['IncorporationYear',
                         'SIMPLE_SIC']]
    ch_short['SIMPLE_SIC'] = ch_short['SIMPLE_SIC']\
        .str.replace('Fire & Insurance', 'FIRE')
    ch_short['SIMPLE_SIC'] = ch_short['SIMPLE_SIC']\
        .str.replace('Real Estate', 'FIRE')
    ch_short = ch_short[ch_short['SIMPLE_SIC']!=None]
#    df_years_sic = pd.DataFrame(index = np.arange(startyear,
#                                                  endyear+1,
#                                                  dtype=np.int),
#                    columns = ['Manufacturing',
#                               'PST'])
    df_years_sic_pc = pd.DataFrame(index=np.arange(startyear, endyear+1,
                                                   dtype=np.int),
                                   columns=['Manufacturing', 'PST'])
    for year in tqdm(df_years_sic_pc.index):
        for sic in list(df_years_sic_pc):
            df = ch_short[ch_short['IncorporationYear'] == str(year)]
#            df_years_sic.loc[year, sic] = len(df[df['SIMPLE_SIC']==sic])
            df_years_sic_pc.loc[year, sic] = (len(df[df['SIMPLE_SIC'] == sic]) /
                                              len(df))
    return df_years_sic_pc, ch_counts_toplot

def gen_gen_toplot(ch_off, ch_psc):

    ch_psc_mean = ch_psc[(ch_psc['isfemale']==1.0) | (ch_psc['isfemale']==0.0)].\
                  groupby(['SICCode.SicText_1'])['isfemale'].mean()
    ch_psc_count = ch_psc[(ch_psc['isfemale']==1.0) | (ch_psc['isfemale']==0.0)].\
                  groupby(['SICCode.SicText_1']).count()
    ch_psc_bysic = pd.merge(pd.DataFrame(ch_psc_mean),
                            pd.DataFrame(ch_psc_count),
                            left_index=True, right_index=True)
    ch_psc_bysic = ch_psc_bysic.rename(columns={'isfemale_x': 'MeanFemale'})
    ch_psc_bysic = ch_psc_bysic.rename(columns={'isfemale_y': 'PSCCount'})
    ch_psc_bysic = ch_psc_bysic[ch_psc_bysic['PSCCount'] > 1000]

    ch_off_mean = ch_off[(ch_off['isfemale']==1.0) | (ch_off['isfemale']==0.0)].\
                  groupby(['SICCode.SicText_1'])['isfemale'].mean()
    ch_off_count = ch_off[(ch_off['isfemale']==1.0) | (ch_off['isfemale']==0.0)].\
                  groupby(['SICCode.SicText_1']).count()
    ch_off_bysic = pd.merge(pd.DataFrame(ch_off_mean),
                            pd.DataFrame(ch_off_count),
                            left_index=True, right_index=True)
    ch_off_bysic = ch_off_bysic.rename(columns={'isfemale_x': 'MeanFemale'})
    ch_off_bysic = ch_off_bysic.rename(columns={'isfemale_y': 'OffCount'})
    ch_off_bysic = ch_off_bysic[ch_off_bysic['OffCount'] > 1000]

    ch_merged_bysic = pd.merge(ch_psc_bysic, ch_off_bysic,
                               left_index=True, right_index=True)
    ch_merged_bysic = ch_merged_bysic.rename(
        columns={'MeanFemale_x': 'MeanFemale_PSC'})
    ch_merged_bysic = ch_merged_bysic.rename(
        columns={'MeanFemale_y': 'MeanFemale_Officer'})

    officer_forenames = pd.DataFrame(ch_off[(ch_off['isfemale']==1.0) |
                                            (ch_off['isfemale']==0.0)].\
                                     groupby(['forename']).size(),
                                     columns=['count_officer_forenames'])
    officer_forenames['count_officer_forenames'] = (
        officer_forenames['count_officer_forenames'] /
        len(ch_off[(ch_off['isfemale']==1.0) | (ch_off['isfemale']==0.0)]))

    psc_forenames = pd.DataFrame(ch_psc[(ch_psc['isfemale']==1.0) |
                                        (ch_psc['isfemale']==0.0)].\
                                     groupby(['forename']).size(),
                                     columns=['count_psc_forenames'])
    psc_forenames['count_psc_forenames'] = (
        psc_forenames['count_psc_forenames'] /
        len(ch_psc[(ch_psc['isfemale']==1.0) | (ch_psc['isfemale']==0)]))

    ch_forenames = pd.merge(officer_forenames,
                            psc_forenames,
                            left_index=True,
                            right_index=True)
    ch_forenames = ch_forenames.sort_values(by='count_officer_forenames', ascending=True)

    off_bycomp_mean = pd.DataFrame(ch_off[(ch_off['isfemale']==1.0) | (ch_off['isfemale']==0.0)].\
                                   groupby(['CompanyNumber'])['isfemale'].mean()
                                   * 100).rename(columns={'isfemale':
                                                          'Female officer (%)'})
    off_bycomp_count = pd.DataFrame(ch_off[(ch_off['isfemale']==1.0) | (ch_off['isfemale']==0.0)].\
                                    groupby(['CompanyNumber'])['isfemale'].count()).\
                       rename(columns={'isfemale': 'Company Count'})
    company_officer_merged = pd.merge(off_bycomp_mean,
                                      off_bycomp_count,
                                      left_index=True,
                                      right_index=True)
    company_officer_merged = pd.DataFrame(company_officer_merged.groupby(
        ['Company Count'])['Female officer (%)'].mean())/100
    psc_bycomp_mean = pd.DataFrame(ch_psc[(ch_psc['isfemale']==1.0) | (ch_psc['isfemale']==0.0)].\
                                   groupby(['company_number'])['isfemale'].mean()
                                   * 100).rename(columns={'isfemale':
                                                          'Female PSC (%)'})
    psc_bycomp_count = pd.DataFrame(ch_psc[(ch_psc['isfemale']==1.0) | (ch_psc['isfemale']==0.0)].\
                                    groupby(['company_number'])['isfemale'].count()).\
                       rename(columns={'isfemale': 'Company Count'})
    company_psc_merged = pd.merge(psc_bycomp_mean,
                                  psc_bycomp_count, how='left',
                                  left_index=True, right_index=True)
    company_psc_merged = pd.merge(psc_bycomp_mean, psc_bycomp_count,
                                  left_index=True, right_index=True)
    company_psc_merged = pd.DataFrame(company_psc_merged.groupby(
        ['Company Count'])['Female PSC (%)'].mean())/100
    companies_merged = pd.merge(company_officer_merged,
                                company_psc_merged,
                                left_index=True,
                                right_index=True)

    return ch_off_bysic, ch_psc_bysic, companies_merged, ch_merged_bysic, ch_forenames


def gen_age_toplot(ch_off, ch_psc, ch_accounts):

    off_bycomp_mean = pd.DataFrame(ch_off[ch_off['Age'].notnull()].\
                                   groupby(['CompanyNumber'])[
                                   'Age'].mean()).rename(columns={'Age':
                                                          'Officer Age'})
    off_bycomp_count = pd.DataFrame(ch_off[ch_off['Age'].notnull()].\
                                    groupby(['CompanyNumber'])[
                                    'Age'].count()).rename(columns={'Age':
                                                                    'Company Count'})
    company_officer_merged = pd.merge(off_bycomp_mean,
                                      off_bycomp_count,
                                      left_index=True,
                                      right_index=True)
    company_officer_merged = pd.DataFrame(company_officer_merged.groupby(
        ['Company Count'])['Officer Age'].mean())
    psc_bycomp_mean = pd.DataFrame(ch_psc[ch_psc['Age'].notnull()].\
                                   groupby(['company_number'])[
                                   'Age'].mean()).rename(columns={'Age':
                                                          'PSC Age'})
    psc_bycomp_count = pd.DataFrame(ch_psc[ch_psc['Age'].notnull()].\
                                    groupby(['company_number'])[
                                    'Age'].count()).rename(columns={'Age':
                                                                    'Company Count'})
    company_psc_merged = pd.merge(psc_bycomp_mean,
                                  psc_bycomp_count, how='left',
                                  left_index=True, right_index=True)
    company_psc_merged = pd.merge(psc_bycomp_mean, psc_bycomp_count,
                                  left_index=True, right_index=True)
    company_psc_merged = pd.DataFrame(company_psc_merged.groupby(
        ['Company Count'])['PSC Age'].mean())
    companies_merged = pd.merge(company_officer_merged,
                                company_psc_merged,
                                left_index=True,
                                right_index=True)

    ch_psc_mean = ch_psc[ch_psc['Age'].notnull()].groupby(['SICCode.SicText_1'])['Age'].mean()
    ch_psc_count = ch_psc[ch_psc['Age'].notnull()].groupby(['SICCode.SicText_1']).count()
    ch_psc_bysic = pd.merge(pd.DataFrame(ch_psc_mean),
                            pd.DataFrame(ch_psc_count),
                            left_index=True, right_index=True)
    ch_psc_bysic = ch_psc_bysic.rename(columns={'Age_x': 'Average PSC Age'})
    ch_psc_bysic = ch_psc_bysic.rename(columns={'Age_y': 'PSCCount'})
    ch_psc_bysic = ch_psc_bysic[ch_psc_bysic['PSCCount'] > 1000]
    ch_off_mean = ch_off[ch_off['Age'].notnull()].groupby(['SICCode.SicText_1'])['Age'].mean()
    ch_off_count = ch_off[ch_off['Age'].notnull()].groupby(['SICCode.SicText_1']).count()
    ch_off_bysic = pd.merge(pd.DataFrame(ch_off_mean),
                            pd.DataFrame(ch_off_count),
                            left_index=True, right_index=True)
    ch_off_bysic = ch_off_bysic.rename(columns={'Age_x':
                                                'Average Officer Age'})
    ch_off_bysic = ch_off_bysic.rename(columns={'Age_y': 'OffCount'})
    ch_off_bysic = ch_off_bysic[ch_off_bysic['OffCount'] > 1000]
    ch_merged_bysic = pd.merge(ch_psc_bysic, ch_off_bysic,
                               left_index=True, right_index=True)

    ch_av_off_age = ch_off[(ch_off['Resigned'].isnull()) &
                           (ch_off['Age'].notnull()) &
                           (ch_off['Officer Role'] == 'director')].\
        groupby(['CompanyNumber'])['Age'].\
        mean().reset_index(name='Average Officer Age')

    ch_av_psc_age = ch_psc.groupby(['company_number'])['Age'].\
        mean().reset_index(name='Average PSC Age')

    ch_av_age = pd.merge(ch_av_off_age,
                         ch_accounts,
                         how='left',
                         left_on='CompanyNumber',
                         right_on='ch_num')
    ch_av_age = pd.merge(ch_av_age,
                         ch_av_psc_age,
                         how='left',
                         left_on='CompanyNumber',
                         right_on='company_number')
    for asset in ['CurrentAssets', 'NetCurrentAssetsLiabilities','TotalAssetsLessCurrentLiabilities']:
        ch_av_age[asset+'_cat'] = 'No Data'
        ch_av_age[asset+'_cat'] = np.where(ch_av_age[asset]<0, '<£0', ch_av_age[asset+'_cat'])
        ch_av_age[asset+'_cat'] = np.where((0<ch_av_age[asset]) & (ch_av_age[asset]<=10000),
                                           '£0-10k', ch_av_age[asset+'_cat'])
        ch_av_age[asset+'_cat'] = np.where((10000<ch_av_age[asset]) & (ch_av_age[asset]<=100000),
                                           '£10k-£100k', ch_av_age[asset+'_cat'])
        ch_av_age[asset+'_cat'] = np.where((100000<ch_av_age[asset]) & (ch_av_age[asset]<=500000),
                                           '£100k-£500k', ch_av_age[asset+'_cat'])
        ch_av_age[asset+'_cat'] = np.where((500000<ch_av_age[asset]) & (ch_av_age[asset]<=1000000),
                                           '£500k-£1m', ch_av_age[asset+'_cat'])
        ch_av_age[asset+'_cat'] = np.where((1000000<ch_av_age[asset]) & (ch_av_age[asset]<=2000000),
                                           '£1m-£2m', ch_av_age[asset+'_cat'])
        ch_av_age[asset+'_cat'] = np.where((2000000<ch_av_age[asset]) & (ch_av_age[asset]<=5000000),
                                           '£2m-£5m', ch_av_age[asset+'_cat'])
        ch_av_age[asset+'_cat'] = np.where(5000000<ch_av_age[asset],
                                           '>£5m', ch_av_age[asset+'_cat'])

    off_bycomp_mean = pd.DataFrame(ch_off.groupby(['CompanyNumber'])['Age'].mean()).rename({'Age':'Officer Age Mean'}, axis=1)
    off_bycomp_sem = pd.DataFrame(ch_off.groupby(['CompanyNumber'])['Age'].sem()).rename({'Age':'Officer Age SEM'}, axis=1)
    off_bycomp_count = pd.DataFrame(ch_off.groupby(['CompanyNumber'])['Age'].count()).\
                       rename(columns={'Age': 'Company Count'})
    company_officer_merged = pd.merge(off_bycomp_mean,
                                      off_bycomp_count,
                                      left_index=True,
                                      right_index=True)
    company_officer_merged = pd.merge(company_officer_merged,
                                      off_bycomp_sem,
                                      left_index=True,
                                      right_index=True)
    company_officer_merged = pd.DataFrame(company_officer_merged.groupby(
        ['Company Count'])[['Officer Age Mean', 'Officer Age SEM']].mean())
    psc_bycomp_mean = pd.DataFrame(ch_psc.groupby(['company_number'])['Age'].mean()).rename({'Age':'PSC Age Mean'}, axis=1)
    psc_bycomp_sem = pd.DataFrame(ch_psc.groupby(['company_number'])['Age'].sem()).rename({'Age':'PSC Age SEM'}, axis=1)
    psc_bycomp_count = pd.DataFrame(ch_psc.groupby(['company_number'])['Age'].count()).\
                       rename(columns={'Age': 'Company Count'})
    company_psc_merged = pd.merge(psc_bycomp_mean,
                                  psc_bycomp_count, how='left',
                                  left_index=True, right_index=True)
    company_psc_merged = pd.merge(psc_bycomp_mean, psc_bycomp_count,
                                  left_index=True, right_index=True)
    company_psc_merged = pd.merge(company_psc_merged , psc_bycomp_sem,
                                  left_index=True, right_index=True)
    company_psc_merged = pd.DataFrame(company_psc_merged.groupby(
        ['Company Count'])[['PSC Age Mean','PSC Age SEM']].mean())
    companies_merged = pd.merge(company_officer_merged,
                                company_psc_merged,
                                left_index=True,
                                right_index=True)
    return ch_merged_bysic, ch_av_age, companies_merged, ch_psc_bysic, ch_off_bysic


def gen_nat_toplot(ch_psc, ch_off, ons_df):
    psc_nat_count = ch_psc[ch_psc['nationality_cleaned'].notnull()].\
                    groupby(['nationality_cleaned'])['nationality_cleaned'].\
                    count().reset_index(name="count")
    psc_nat_merged = pd.merge(psc_nat_count,
                              ons_df,
                              how='left',
                              left_on='nationality_cleaned',
                              right_on='nationality_cleaned')
    psc_nat_merged = psc_nat_merged[(psc_nat_merged['nationality_cleaned']!='N\A')].\
                                    sort_values(by='count',ascending=False)
    psc_nat_merged['per person'] = (psc_nat_merged['count']/psc_nat_merged['uk_pop']).\
                                   round(decimals=2)

    off_nat_count = ch_off[ch_off['nationality_cleaned'].notnull()].\
                    groupby(['nationality_cleaned'])['nationality_cleaned'].\
                    count().reset_index(name="count")
    off_nat_merged = pd.merge(off_nat_count,
                              ons_df,
                              how='left',
                              left_on='nationality_cleaned',
                              right_on='nationality_cleaned')
    off_nat_merged = off_nat_merged[(off_nat_merged['nationality_cleaned']!='N\A')].\
                                    sort_values(by='count', ascending=False)
    off_nat_merged['per person'] = (off_nat_merged['count']/off_nat_merged['uk_pop']).\
                                   round(decimals=2)

    psc_nat_merged = pd.merge(psc_nat_merged,
                              ch_psc[(ch_psc['nationality_cleaned'].notnull()) &
                                     (ch_psc['isfemale'].notnull())].\
                              groupby(['nationality_cleaned'])['isfemale'].\
                              mean().reset_index(name="average female"),
                              how='left',
                              left_on='nationality_cleaned',
                              right_on='nationality_cleaned')

    off_nat_merged = pd.merge(off_nat_merged,
                              ch_off[(ch_off['nationality_cleaned'].notnull()) &
                                     (ch_off['isfemale'].notnull())].\
                              groupby(['nationality_cleaned'])['isfemale'].\
                              mean().reset_index(name="average female"),
                              how='left',
                              left_on='nationality_cleaned',
                              right_on='nationality_cleaned')

    psc_nat_merged = pd.merge(psc_nat_merged,
                              ch_psc[(ch_psc['nationality_cleaned'].notnull()) &
                                     (ch_psc['Age'].notnull())].\
                              groupby(['nationality_cleaned'])['Age'].mean().\
                              reset_index(name="Average Age"),
                              how='left',
                              left_on='nationality_cleaned',
                              right_on='nationality_cleaned')

    off_nat_merged = pd.merge(off_nat_merged,
                              ch_off[(ch_off['nationality_cleaned'].notnull()) &
                                     (ch_off['Age'].notnull())].\
                              groupby(['nationality_cleaned'])['Age'].mean().\
                              reset_index(name="Average Age"),
                              how='left',
                              left_on='nationality_cleaned',
                              right_on='nationality_cleaned')

    return off_nat_merged, psc_nat_merged


def gen_res_toplot(ch_off, ch_psc, map_london_df, map_uk_df, ch_accounts):

    ch_off_grp_dist = ch_off[ch_off['District'].notnull()].\
                      groupby(['District']).size().\
                      sort_values(ascending=False)
    ch_off_grp_dist = ch_off_grp_dist.reset_index().\
        rename(columns={0: 'Officer Count'})
    ch_off_grp_dist['Total Officers (%)'] = (ch_off_grp_dist['Officer Count'] /
                                             ch_off_grp_dist['Officer Count'].sum())*100
    ch_off_grp_NUTS = ch_off[ch_off['NUTS118NM'].notnull()].\
        groupby(['NUTS118NM']).size().\
        sort_values(ascending=False)
    ch_off_grp_NUTS = ch_off_grp_NUTS.reset_index().\
       rename(columns={0: 'Officer Count'})
    ch_off_grp_NUTS['Total Officers (%)'] = (ch_off_grp_NUTS['Officer Count'] /
                                             ch_off_grp_NUTS['Officer Count'].sum())*100
    ch_psc_grp_dist = ch_psc[ch_psc['District'].notnull()].\
        groupby(['District']).size().\
        sort_values(ascending=False)
    ch_psc_grp_dist = ch_psc_grp_dist.reset_index().\
        rename(columns={0: 'PSC Count'})
    ch_psc_grp_dist['Total PSC (%)'] = (ch_psc_grp_dist['PSC Count'] /
                                        ch_psc_grp_dist['PSC Count'].sum())*100
    ch_psc_grp_NUTS = ch_psc[ch_psc['NUTS118NM'].notnull()].\
        groupby(['NUTS118NM']).size().\
        sort_values(ascending=False)
    ch_psc_grp_NUTS = ch_psc_grp_NUTS.reset_index().\
        rename(columns={0: 'PSC Count'})
    ch_psc_grp_NUTS['Total PSC (%)'] = (ch_psc_grp_NUTS['PSC Count'] /
                                        ch_psc_grp_NUTS['PSC Count'].sum())*100
    ch_off_curnetpp = pd.merge(ch_off[ch_off['Resigned'].notnull()],
                               pd.DataFrame(ch_off[ch_off['Resigned'].notnull()].groupby('CompanyNumber')['CompanyNumber'].count())\
                               .rename({'CompanyNumber':'Officer Count'}, axis=1).reset_index(),
                               how='left',
                               left_on='CompanyNumber',
                               right_on='CompanyNumber')
    ch_off_curnetpp = pd.merge(ch_off_curnetpp,
                               ch_accounts,
                               how = 'left',
                               left_on = 'CompanyNumber',
                               right_on = 'ch_num')
    ch_off_curnetpp['CurNetLiaAssetsPOff'] = ch_off_curnetpp['NetCurrentAssetsLiabilities']/ch_off_curnetpp['Officer Count']
    ch_off_curnetpp = ch_off_curnetpp[ch_off_curnetpp['CurNetLiaAssetsPOff'].notnull()].groupby(['NUTS118NM'])['CurNetLiaAssetsPOff'].sum().sort_values(ascending=False)
    ch_off_grp_NUTS = pd.merge(ch_off_grp_NUTS,
                               pd.DataFrame(ch_off_curnetpp),
                               how = 'left',
                               left_on = 'NUTS118NM',
                               right_on = 'NUTS118NM')
    ch_psc_curnetpp = pd.merge(ch_psc,
                               pd.DataFrame(ch_psc.groupby('company_number')['company_number'].count())\
                               .rename({'company_number':'PSC Count'}, axis=1).reset_index(),
                               how='left',
                               left_on='company_number',
                               right_on='company_number')
    ch_psc_curnetpp = pd.merge(ch_psc_curnetpp,
                               ch_accounts,
                               how = 'left',
                               left_on = 'company_number',
                               right_on = 'ch_num')
    ch_psc_curnetpp['CurNetLiaAssetsPPSC'] = ch_psc_curnetpp['NetCurrentAssetsLiabilities']/ch_psc_curnetpp['PSC Count']
    ch_psc_curnetpp = ch_psc_curnetpp[ch_psc_curnetpp['CurNetLiaAssetsPPSC'].notnull()].groupby(['NUTS118NM'])['CurNetLiaAssetsPPSC'].sum().sort_values(ascending=False)
    ch_psc_grp_NUTS = pd.merge(ch_psc_grp_NUTS,
                               pd.DataFrame(ch_psc_curnetpp),
                               how = 'left',
                               left_on = 'NUTS118NM',
                               right_on = 'NUTS118NM')

    ch_psc_grp_NUTS['CurNetAssPPSC (%)'] = (ch_psc_grp_NUTS['CurNetLiaAssetsPPSC']/ch_psc_grp_NUTS['CurNetLiaAssetsPPSC'].sum())*100
    ch_off_grp_NUTS['CurNetAssPOff (%)'] = (ch_off_grp_NUTS['CurNetLiaAssetsPOff']/ch_off_grp_NUTS['CurNetLiaAssetsPOff'].sum())*100

    map_london_df = pd.merge(map_london_df,
                             ch_off_grp_dist,
                             how='left',
                             left_on='NAME',
                             right_on='District')
    map_london_df = pd.merge(map_london_df,
                             ch_psc_grp_dist,
                             how='left',
                             left_on='NAME',
                             right_on='District')
    ch_psc_with_geo_nolondon = ch_psc[(ch_psc['District'].notnull()) &
                                      (ch_psc['Postcode Area'].notnull())]\
                                     [~ch_psc[(ch_psc['District'].notnull()) &
                                      (ch_psc['Postcode Area'].notnull())].
                                      District.isin(map_london_df.NAME)]
    ch_off_with_geo_nolondon = ch_off[(ch_off['District'].notnull()) &
                                      (ch_off['Postcode Area'].notnull())]\
                                     [~ch_off[(ch_off['District'].notnull()) &
                                       (ch_off['Postcode Area'].notnull())].
                                     District.isin(map_london_df.NAME)]
    ch_psc_with_geo_nolondon_grp = ch_psc_with_geo_nolondon.\
        groupby(['Postcode Area']).size()
    ch_off_with_geo_nolondon_grp = ch_off_with_geo_nolondon.\
       groupby(['Postcode Area']).size()
    PCode_Area_Counts_nolondon = pd.merge(pd.DataFrame(ch_psc_with_geo_nolondon_grp,
                                                       columns=['PSC Count']),
                                          pd.DataFrame(ch_off_with_geo_nolondon_grp,
                                                       columns=['Officer Count']),
                                          left_index=True, right_index=True)
    PCode_Area_Counts_nolondon['Non-London Officers (%)'] = (PCode_Area_Counts_nolondon['Officer Count'] /
                                                             PCode_Area_Counts_nolondon['Officer Count'].\
                                                             sum())*100
    PCode_Area_Counts_nolondon['Non-London PSC (%)'] = (PCode_Area_Counts_nolondon['PSC Count'] /
                                                        PCode_Area_Counts_nolondon['PSC Count'].sum())*100
    PCode_Area_Counts_nolondon = PCode_Area_Counts_nolondon.sort_values(by='Officer Count',
                                                                        ascending=False)
    map_uk_df_merged = pd.merge(map_uk_df.set_index('name'),
                                PCode_Area_Counts_nolondon,
                                how='left',
                                left_index=True,
                                right_index=True)
    return map_uk_df_merged, ch_psc_grp_NUTS, ch_off_grp_NUTS,  map_london_df

from ch_supportfunctions import (
    make_timely_gender,
    make_gender,
    make_age,
    send_df_to_clean_for_nat)
import pandas as pd
import numpy as np
import gender_guesser.detector as gender
gendet = gender.Detector()


def engineer_gender(ch_off, ch_psc, ch_basic):
    ch_off['Name'] = np.where(ch_off['Name'] != 'N\A',
                              ch_off['Name'], np.nan)
    ch_psc['forename'] = np.where(ch_psc['forename'] != 'N\A',
                                  ch_psc['forename'], np.nan)
    ch_psc, ch_off = make_gender(ch_psc, ch_off, ch_basic)
    sic_timely_off_gender, timely_df_a_gender = make_timely_gender(ch_psc,
                                                                   ch_off)
    ch_psc['isfemale'] = np.where(ch_psc['gender'] == 'female',
                                  1.0,
                                  np.nan)
    ch_psc['isfemale'] = np.where(ch_psc['gender'] == 'male',
                                  0.0,
                                  ch_psc['isfemale'])
    ch_off['isfemale'] = np.where(ch_off['gender'] == 'female',
                                  1.0,
                                  np.nan)
    ch_off['isfemale'] = np.where(ch_off['gender'] == 'male',
                                  0.0,
                                  ch_off['isfemale'])
    print('We observe ' +
          str(len(ch_off[(ch_off['isfemale'] == 1.0) |
                         (ch_off['isfemale'] == 0.0)])) +
          ' rows of Officer data which cannot be determined as m/f (' +
          str((len(ch_off[(ch_off['isfemale'] == 1.0) |
                          (ch_off['isfemale'] == 0.0)]) / len(ch_off)) * 100) +
          '%).')
    print('We observe ' +
          str(len(ch_psc[(ch_psc['isfemale'] == 1.0) |
                         (ch_psc['isfemale'] == 0.0)])) +
          ' rows of PSC data which cannot be determined as m/f (' +
          str((len(ch_psc[(ch_psc['isfemale'] == 1.0) |
                          (ch_psc['isfemale'] == 0.0)]) / len(ch_psc)) * 100) +
          '%).')
    print('Length of ch_off: ' + str(len(ch_off)) + '.\n' +
          'Length of ch_psc: ' + str(len(ch_psc)) + '.')

    return ch_off, ch_psc, sic_timely_off_gender, timely_df_a_gender


def engineer_age(ch_off, ch_psc):
    ''' docstrings here '''
    ch_off = ch_off.rename({'Date of Birth': 'date_of_birth'}, axis='columns')
    ch_off = make_age(ch_off, 'Age', pd.to_datetime('today'))
    ch_psc = make_age(ch_psc, 'Age', pd.to_datetime('today'))
    ch_off = ch_off.rename({'Date of Birth':
                            'date_of_birth'}, axis='columns')
    ch_off = make_age(ch_off, 'Resigned_Age', ch_off['Resigned_DT'])
    ch_off = make_age(ch_off, 'Appointed_Age', ch_off['Appointed_DT'])
    print('We observe ' +
          str(len(ch_off[ch_off['Age'].isnull()])) +
          ' rows of Officer data which have null age (' +
          str((len(ch_off[ch_off['Age'].isnull()]) / len(ch_off)) * 100) +
          '%).')
    print('We observe ' +
          str(len(ch_psc[ch_psc['Age'].isnull()])) +
          ' rows of PSC data which have null age (' +
          str((len(ch_psc[ch_psc['Age'].isnull()]) / len(ch_psc)) * 100) +
          '%).')
    print('Length of ch_off: ' + str(len(ch_off)) + '.\n' +
          'Length of ch_psc: ' + str(len(ch_psc)) + '.')
    return ch_off, ch_psc


def engineer_residences(ch_off, ch_psc, dist_to_nuts, postcodes):

    ch_off = pd.merge(ch_off,
                      postcodes[['Postcode',
                                 'District',
                                 'LSOA Code',
                                 'Country',
                                 'Index of Multiple Deprivation']],
                      how='left',
                      left_on='postal_code',
                      right_on='Postcode')
    ch_off = pd.merge(ch_off,
                      dist_to_nuts[['NUTS118NM', 'LAD16NM']],
                      right_on='LAD16NM',
                      left_on='District',
                      how='left')

    ch_psc = pd.merge(ch_psc,
                      postcodes[['Postcode',
                                 'District',
                                 'LSOA Code',
                                 'Country',
                                 'Index of Multiple Deprivation']],
                      how='left',
                      left_on='postal_code',
                      right_on='Postcode')
    ch_psc = pd.merge(ch_psc,
                      dist_to_nuts[['NUTS118NM', 'LAD16NM']],
                      right_on='LAD16NM',
                      left_on='District',
                      how='left')

    ch_off['First_Part_PCode'] = pd.DataFrame(ch_off['postal_code'].
                                              str.split(' ',
                                                        expand=True))[0]
    df_PCodes = pd.concat([pd.Series(ch_off['First_Part_PCode'].
                                     unique()).
                           rename("First_Part_PCode"),
                           pd.Series(ch_off['First_Part_PCode'].
                                     unique()).
                           str.findall(r'[A-Za-z]+|\d+').apply(pd.Series)],
                          axis=1)
    ch_off = pd.merge(ch_off, df_PCodes, left_on='First_Part_PCode',
                      right_on='First_Part_PCode', how='left')
    ch_off = ch_off.rename(index=str,
                           columns={0: "Postcode Area",
                                    1: 'Second Part Postcode Area Split',
                                    2: 'Third Part Postcode Area Split'})

    ch_psc['First_Part_PCode'] = pd.DataFrame(ch_psc['postal_code'].
                                              str.split(' ',
                                                        expand=True))[0]
    df_PCodes = pd.concat([pd.Series(ch_psc['First_Part_PCode'].
                                     unique()).
                           rename("First_Part_PCode"),
                           pd.Series(ch_psc['First_Part_PCode'].
                                     unique()).
                           str.findall(r'[A-Za-z]+|\d+').apply(pd.Series)],
                          axis=1)
    ch_psc = pd.merge(ch_psc, df_PCodes, left_on='First_Part_PCode',
                      right_on='First_Part_PCode', how='left')
    ch_psc = ch_psc.rename(index=str,
                           columns={0: "Postcode Area",
                                    1: 'Second Part Postcode Area Split',
                                    2: 'Third Part Postcode Area Split'})
    ch_off['District'] = ch_off['District'].str.\
        replace('Shepway', 'Folkestone and Hythe')
    ch_psc['District'] = ch_psc['District'].str.\
        replace('Shepway', 'Folkestone and Hythe')
    print('We observe ' +
          str(len(ch_psc[ch_psc['District'].isnull()])) +
          ' rows of PSC data which are not successfully ' +
          ' mapped to districts (' +
          str((len(ch_psc[ch_psc['District'].isnull()]) /
               len(ch_psc)) * 100) + '%).')
    print('We observe ' + str(len(ch_off[ch_off['District'].isnull()])) +
          ' rows of Officer data which are not successfully' +
          ' mapped to districts (' +
          str((len(ch_off[ch_off['District'].isnull()]) /
               len(ch_off)) * 100) + '%).')
    print('We observe ' +
          str(len(ch_psc[ch_psc['postal_code'].isnull()])) +
          ' rows of PSC data which are not successfully ' +
          'mapped to postcodes (' +
          str((len(ch_psc[ch_psc['postal_code'].isnull()]) /
               len(ch_psc)) * 100) + '%).')
    print('We observe ' + str(len(ch_off[ch_off['postal_code'].isnull()])) +
          ' rows of Officer data which are not successfully'
          ' mapped to postcodes (' +
          str((len(ch_off[ch_off['postal_code'].isnull()]) /
               len(ch_off)) * 100) + '%).')
    print('Length of ch_off: ' + str(len(ch_off)) + '.\n' +
          'Length of ch_psc: ' + str(len(ch_psc)) + '.')
    return ch_off, ch_psc


def engineer_nat(ch_off, ch_psc, anom_remove, anom_replace):
    ch_off = ch_off.rename({'Nationality': 'nationality'}, axis='columns')
    ch_off = send_df_to_clean_for_nat(ch_off, anom_remove, anom_replace)
    ch_psc = send_df_to_clean_for_nat(ch_psc, anom_remove, anom_replace)
    print('We observe ' +
          str(len(ch_off[ch_off['nationality_cleaned'].isnull()])) +
          ' rows of Officer data which are not successfully' +
          ' mapped to Nationalities (' +
          str((len(ch_off[ch_off['nationality_cleaned'].isnull()]) /
               len(ch_off)) * 100) + '%).')
    print('We observe ' +
          str(len(ch_psc[ch_psc['nationality_cleaned'].isnull()])) +
          ' rows of Officer data which are not successfully' +
          ' mapped to Nationalities (' +
          str((len(ch_psc[ch_psc['nationality_cleaned'].isnull()]) /
               len(ch_psc)) * 100) + '%).')
    print('Length of ch_off: ' + str(len(ch_off)) + '.\n' +
          'Length of ch_psc: ' + str(len(ch_psc)) + '.')
    return ch_off, ch_psc

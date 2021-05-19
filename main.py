from datetime import datetime, timedelta
from IO import Import, ExportCSV, ExportMapFRMetro
from Summarize import Sum, Average
from Transform import AsNumeric, Rename, Select, GroupBy, Sort, Mutate, Filter, MovingAverage, KMeans, Round
from Pipeline import Pipeline


if __name__ == '__main__':
    hospit_nouveaux = Import.import_csv("data/donnees-hospitalieres-nouveaux-covid19-2021-03-03-17h03.csv")
    donnees_vacances = Import.import_json("data/VacancesScolaires.json", "Calendrier")

    # Quel est le nombre total d’hospitalisations dues au Covid-19 ?
    num_hospit = AsNumeric('incid_hosp')
    total_hospit = Sum('incid_hosp')
    answer_q1 = Pipeline(num_hospit, total_hospit)
    print(answer_q1.apply(hospit_nouveaux)['incid_hosp_Sum', 0])  # Réponse attendue : 366449

    # Combien de nouvelles hospitalisations ont eu lieu ces 7 derniers jours dans chaque département ?
    groupe_dep = GroupBy('dep')
    tri_jour_desc = Sort('desc_jour')
    creer_index_ligne = Mutate(ligne='row_index')
    filter_index_ligne = Filter(ligne='<=6')
    export_res_q2 = ExportCSV('answer_q2.csv')
    answer_q2 = Pipeline(num_hospit, groupe_dep, tri_jour_desc, creer_index_ligne,
                         filter_index_ligne, total_hospit, export_res_q2)
    answer_q2.apply(hospit_nouveaux)  # Réponse attendue : voir table 'docs/answer_q2.csv'

    # Comment évolue la moyenne des nouvelles hospitalisations journalières de cette semaine
    # par rapport à celle de la semaine dernière ?
    groupe_jour = GroupBy('jour')
    moy_mobile = MovingAverage(7, 'jour', 'incid_hosp_Sum')
    comparer_semaines = Filter(ligne=' in [0,7]')
    renommer_vars_hospi = Rename(Hospi='incid_hosp_Sum', MoyMobile='incid_hosp_Sum_MA7')
    selectionner_vars_hospi = Select('jour', 'Hospi', 'MoyMobile')
    calculer_evolution = Mutate(Evolution='100*(MoyMobile-lead_MoyMobile)/lead_MoyMobile')
    arrondi = Round('MoyMobile', 'Evolution', precision=4)
    answer_q3 = Pipeline(num_hospit, groupe_jour, total_hospit, moy_mobile,
                         tri_jour_desc, creer_index_ligne, comparer_semaines,
                         renommer_vars_hospi, selectionner_vars_hospi, calculer_evolution, arrondi)
    print(answer_q3.apply(hospit_nouveaux)['Evolution', 0])  # Réponse attendue : -0.5937 (%)

    # Quel est le résultat de k-means avec k=3 sur les données des départements du mois de Janvier 2021,
    # lissées avec une moyenne glissante de 7 jours ?
    num_data = AsNumeric('incid_hosp', 'incid_rea', 'incid_dc', 'incid_rad')
    filtrer_dates = Filter(jour=">= '2020-12-26' and jour <= '2021-01-31'")
    moy_mobiles = MovingAverage(7, 'jour', 'incid_hosp', 'incid_rea', 'incid_dc', 'incid_rad')
    moy_ma = Average('incid_hosp_MA7', 'incid_rea_MA7', 'incid_dc_MA7', 'incid_rad_MA7')
    clustering = KMeans(3, 'incid_hosp_MA7_Average', 'incid_rea_MA7_Average',
                        'incid_dc_MA7_Average', 'incid_rad_MA7_Average', random_seed=20)
    map_cluster = ExportMapFRMetro('answer_q4.png', 'departement', 'dep', 'Partition',
                                   title="Clustering sur les données de janvier 2021", display_scale=False)
    answer_q4 = Pipeline(num_data, filtrer_dates, groupe_dep, moy_mobiles,
                         moy_ma, clustering, map_cluster)
    answer_q4.apply(hospit_nouveaux)  # Réponse attendue : voir carte 'docs/answer_q4.png' (avec seed=20)

    # Combien de nouvelles admissions en réanimation ont eu lieupendant la semaine suivant les
    # vacances de la Toussaint de 2020 ?
    filtrer_toussaint = Filter(Description="=='Vacances de la Toussaint'", Debut=">='2020-01-01'")
    dates_toussaint = filtrer_toussaint.apply(donnees_vacances)
    fin_toussaint = dates_toussaint['Fin', 0]
    fin_toussaint_plus1sem = datetime.strftime(datetime.strptime(fin_toussaint, '%Y-%m-%d') + timedelta(days=7),
                                               '%Y-%m-%d')
    filtrer_sem_apres_toussaint = Filter(jour=">='" + fin_toussaint + "' and jour <= '" +
                                              fin_toussaint_plus1sem + "'")
    num_rea = AsNumeric('incid_rea')
    somme_rea = Sum('incid_rea')
    answer_q5 = Pipeline(num_rea, filtrer_sem_apres_toussaint, somme_rea)
    print(answer_q5.apply(hospit_nouveaux)['incid_rea_Sum', 0])  # Réponse attendue : 3521

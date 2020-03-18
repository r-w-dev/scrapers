df = pd.read_csv('C:\\Users\\Datalab\\meester_baan.csv', delimiter=';',
                 encoding="ISO-8859-1")

##tussenstap: verwijder de regels waar niets in staat:
df['Plaatsingsdatum'].replace('', np.nan, inplace=True)
df.dropna(subset=['Plaatsingsdatum'], inplace=True)

## Maak extra variabelen aan.
df['Plaatsingsdatum'] = pd.to_datetime(df['Plaatsingsdatum'], format='%d-%m-%Y')
df['Peildatum'] = pd.to_datetime(time.strftime("%x"))
df['Tijd online (dagen)'] = df['Peildatum'] - df['Plaatsingsdatum']
df.head()

# filter op metropoolregio amsterdam:
file = "MRA_plaatsen.csv"
mra = pd.read_csv(DIR + file, delimiter=';', encoding="Latin-1")

df_amsterdam = df.loc[df['Plaats'].isin(mra["Woonplaatsen"])]

# Filter op onderwijssoort
df_amsterdam_filtered = df_amsterdam[df_amsterdam['Sector'].str.contains(
    "Speciaal (Basis) Onderwijs|Basisonderwijs|Voortgezet onderwijs|Middelbaar beroepsonderwijs|Voortgezet speciaal onderwijs") == True]
df_amsterdam_filtered.head()

# Enkele namen aanpassen om koppeling met DUO-lijst mogelijk te maken:
# Kan zijn dat dit moet worden aangevuld met extra scholen (die op meesterbaan.nl staan) die niet gematched kunnen worden met scholen in de DUO-data (ow_instellingen_verrijkt_besturen.csv).
df_amsterdam_filtered['Naam school'] = df_amsterdam_filtered['Naam school'].replace({
                                                                                        "St. Nicolaaslyceum": "Scholengemeenschap Sint Nicolaas Lyceum voor Lyceum en Havo",
                                                                                        'Havo De Hof': "Locatie, De Hof",
                                                                                        'Daltonschool Neptunus': 'Oecumenische Dalton bassischool Neptunus',
                                                                                        'Cheider': 'Stichting Joodse Kindergemeenschap Cheider',
                                                                                        'De Apollo': 'locatie, De Apollo',
                                                                                        'Oscar Carré': 'Basisschool Oscar Carre',
                                                                                        'De Bïenkorf': 'Basisschool De Bienkorf',
                                                                                        'St. Jan': 'St Janschool',
                                                                                        'Daltonschool Aldoende': 'Aldoende',
                                                                                        'St. Nicolaaslyceum': 'Scholengemeenschap Sint Nicolaas Lyceum voor Lyceum en Havo',
                                                                                        'basisschool Polsstok': 'Polsstok',
                                                                                        'JSG Maimonides': 'Joodse Scholengemeenschap Maimonides voor Ath Mavo En Afd Havo',
                                                                                        'Daltonschool Neptunus': 'Oecumenische Dalton bassischool Neptunus',
                                                                                        'Boekmanschool Dr. E. hoofdvestiging': 'Dr E Boekman',
                                                                                        'de Henricus': 'Rooms Katholieke Basisschool Sint Henricusschool',
                                                                                        'De Ark': 'Oecumenische Basisschool De Ark',
                                                                                        'BS Elout': 'Basisschool de Elout',
                                                                                        '3e Daltonschool Alberdingk Thijm': 'Alberdingk Thijmschool Daltonschool',
                                                                                        'Montessori Lyceum Amsterdam': 'Montessori Scholengemeenschap Amsterdam voor Lyceum Havo Mavo Vbo Lwoo',
                                                                                        'Mgr. Bekkersschool': 'Basisschool Mgr Bekkers',
                                                                                        'Kunstmagneetschool De Kraal': 'De Kraal',
                                                                                        'Open Schoolgemeenschap Bijlmer': 'Open Sgm Bylmer',
                                                                                        'Alles in 1 school De Zeeheld': 'Brede School Zeeheld',
                                                                                        'Christelijke Scholengemeenschap Buitenveldert': 'Chr Sgm Buitenveldert',
                                                                                        'basisschool Klaverblad': 'Klaverblad',
                                                                                        'BS Fiep Westendorp': 'Brede school Fiep Westendorp',
                                                                                        'De Nicolaas Maesschool ': 'Nicolaas Maes',
                                                                                        'Al Maes': 'Basisschool Al Maes',
                                                                                        'al Maes': 'Basisschool Al Maes',
                                                                                        'De Buikslotermeer': 'Buikslotermeer',
                                                                                        'Basisschool Annie M G Schmidt': 'Brede School Annie MG Schmidt',
                                                                                        'Obs JP Coen': 'Jan Pietersz. Coenschool',
                                                                                        '2e Daltonschool Pieter Bakkum': 'Pieter Bakkumschool Daltonschool',
                                                                                        'De Vlaamse Reus': 'Openbare Basisschool de Vlaamse Reus',
                                                                                        'Montessori Boven \'t IJ': 'Basisschool Boven \'t IJ Montessori-School',
                                                                                        'Dalton IKC Zeven ZeeÃ«n': 'IKC Zeven ZeeÃ«n',
                                                                                        'IKC NoordRijk ': 'OBS NoordRijk IKC',
                                                                                        'Montessorischool de Eilanden': 'Stichting Montessori Basisschool de Eilanden',
                                                                                        'Kindcentrum De Kaap': 'Openbare Basisschool De Kaap',
                                                                                        'De IJdoornschool': 'IJdoorn-School',
                                                                                        'Huibersschool': 'Basisschool Huibers',
                                                                                        'Brede School de Springplank': 'Katholieke Basisschool De Springplank',
                                                                                        'IJpleinschool': 'Openbare Bassischool IJPlein',
                                                                                        'Barbaraschool': 'Sint Barbara-School Basisschool',
                                                                                        'KPC De Atlant': 'Kolom Praktijkcollege De Atlant',
                                                                                        'BS Fiep Westendorp': 'Brede school Fiep Westendorp',
                                                                                        'Jenaplanschool Atlantis': 'Oecumenische Jenaplan basisschool Atlantis',
                                                                                        'Kameleon': 'Openbare Basisschool De Kameleon',
                                                                                        'Buitenhout College': 'Oostvaarders College voor Lyceum Havo Mavo Vbo Lwoolocatie Buitenhout',
                                                                                        'Polderhof': 'Basisschool De Polderhof',
                                                                                        'Letterland': 'OBS Letterland',
                                                                                        'Het Palet': 'Openbare Basisschool Het Palet',
                                                                                        'Aurora': 'Openbare Basisschool Aurora',
                                                                                        'Argonaut': 'Basisschool De Argonaut',
                                                                                        'LUMION': 'Calandlyceum voor Gymnasium Atheneum Havo Vmbo locatie,  Lumion',
                                                                                        'Joseph Lokinschool': 'Joseph Lokin Basisschool',
                                                                                        'Praktijkschool Oost ter Hout': 'School voor Praktische Vorming Oost-ter-Hout',
                                                                                        'Lyceum Sancta Maria': 'Sancta Maria Lyceum voor Havo, Atheneum en Gymnasium',
                                                                                        'Technisch College Velsen/Maritiem College IJmuiden': 'Noordzee Onderwijs Groep- locatie Technisch College Velsen',
                                                                                        'Trias VMBO': 'Scholengroep Krommenie - locatie Trias VMBO',
                                                                                        'ISG Arcus': 'Interconfessionele Scholengemeenschap Arcus voor Atheneum Havo Mavo Vbo Lwoo',
                                                                                        'SG Antoni GaudÃ­': 'Purmerendse Scholengroep locatie SG. Antoni Gaudi',
                                                                                        'SWV VO Waterland': 'Waterlandschool',
                                                                                        'Jan van Egmond Lyceum': 'Purmerendse Scholengroep locatie Jan v. Egmond Lyceum',
                                                                                        'Pascal Zuid': 'Pascal College  voor Vwo Havo Mavo Vbo Lwoo',
                                                                                        'Sint-Vituscollege': 'Sint-Vitusmavo',
                                                                                        'Basisschool Al Ihsaan': 'Al-Ihsaan',
                                                                                        'Coornhert Lyceum': 'Coornhert Gymnasium',
                                                                                        'De Josephschool': 'Rooms Katholieke Basisschool St Joseph',
                                                                                        'De School': 'Sociocratische school De School',
                                                                                        'De Egelantier': 'Openbare Basisschool De Egelantier',
                                                                                        'de Wilge': 'Rooms Katholieke Basisschool De Wilge',
                                                                                        'Coornhert Lyceum ': 'Coornhertlyc Gem Scholengemeenschap Lyceum Havo Mavo'})

# Lijst DUO onderwijsinstellingen importeren:
# Dit is een aangepaste lijst waar enkele regels uit verwijderd zijn (als een school meerdere locaties heeft, verwijderd na overleg met Ralph)
# ow_instellingen = pd.read_csv('G:/OIS/Projecten/lopende Projecten/18148 Scrapen website lerarentekort/data/Verrijken DUOdata/ow_instellingen_verrijkt_besturen.csv', delimiter=';', encoding = "ISO-8859-1")
file = "ow_instellingen_verrijkt_besturen.csv"
ow_instellingen = pd.read_csv(dir + file, delimiter=';', encoding="ISO-8859-1")
ow_instellingen = ow_instellingen[
    ['NAAM_VOLLEDIG', 'NAAM_KORT', 'adres', 'plaats', 'locatie', 'BRIN_NUMMER']]

"""
#voeg handmatig aantal scholen toe nav  mail Ralph 09/08/2018
nieuwelijst = pd.read_csv('C:/Users/Datalab/Documents/Scrapen Onderwijs/Verrijken DUOdata/180809 aanvullingen schooladressen.csv', delimiter=';', encoding = "ISO-8859-1")
nieuwelijst.rename(index=str, columns={"Plaats": "plaats"}, inplace=True)
ow_instellingen = ow_instellingen.append(nieuwelijst)
ow_instellingen = ow_instellingen[['NAAM_VOLLEDIG', 'NAAM_KORT', 'adres', 'plaats', 'locatie', 'BRIN_NUMMER']]
"""

# toevoegen van MRA vestigingen
# ow_instellingenMRA = pd.read_csv("G:/OIS/Projecten/lopende Projecten/18148 Scrapen website lerarentekort/data/Verrijken DUOdata/MRA scholen Meesterbaan-scrape.csv", delimiter=';', encoding = "ISO-8859-1")
file = "MRA scholen Meesterbaan-scrape.csv"
ow_instellingenMRA = pd.read_csv(dir + file, delimiter=';', encoding="ISO-8859-1")

# ow_instellingenMRA = pd.read_csv("C:/Users/Datalab/Documents/Scrapen Onderwijs/MRA scholen Meesterbaan-scrape.csv", delimiter=';', encoding = "ISO-8859-1") 
#hier zitten alle scholen in buiten amsterdam (ook Assen bijv), maar op zich geen probleem. Kan eventueel nog selectie MRA-plaatsen.
ow_instellingenMRA['adres'] = ow_instellingenMRA["STRAATNAAM"] + " " + \
                              ow_instellingenMRA["HUISNUMMER-TOEVOEGING"]
ow_instellingenMRA.rename(index=str, columns={"VESTIGINGSNUMMER": "BRIN_NUMMER",
                                              "VESTIGINGSNAAM": "NAAM_VOLLEDIG",
                                              "PLAATSNAAM": "plaats"}, inplace=True)
ow_instellingenMRA['NAAM_KORT'] = ""
ow_instellingenMRA['locatie'] = ""
ow_instellingenMRA = ow_instellingenMRA[
    ['NAAM_VOLLEDIG', 'NAAM_KORT', 'adres', 'plaats', 'locatie', 'BRIN_NUMMER']]

ow_instellingen = pd.DataFrame(ow_instellingen)
ow_instellingenMRA = pd.DataFrame(ow_instellingenMRA)
frames = [ow_instellingen, ow_instellingenMRA]
ow_instellingen = pd.concat(frames)
ow_instellingen.loc[(ow_instellingen['plaats'] == "Amsterdam"), 'plaats'] = "Amsterdam"
# ow_instellingen.loc[(ow_instellingen['plaats'] == "Amsterdam"), 'plaats'] = "AMSTERDAM"
#nu is alles in kapitalen, nodig voor match met mra plaatsen.

# mra["Woonplaatsen"] = mra["Woonplaatsen"].str.upper()
mra["Woonplaatsen"] = mra.Woonplaatsen.str.title()
ow_instellingen["plaats"] = ow_instellingen.plaats.str.title()

# selecteer alleen MRA plaatsen:
ow_instellingen = ow_instellingen.loc[
    ow_instellingen['plaats'].isin(mra["Woonplaatsen"])]

##
matching_names_df = pd.DataFrame(columns=('Naam school', 'Naam DUO', 'Match'))

##tijd hoe lang fuzzy matching duurt opnemen.
start = time.time()

# foutmelding datapunt, nieuwe poging:
for index_a, row_a in df_amsterdam_filtered.iterrows():
    for index_b, row_b in ow_instellingen.iterrows():
        if fuzz.partial_ratio(row_a['Naam school'].encode('utf-8'),
                              row_b['NAAM_VOLLEDIG'].encode('utf-8')) > 85:
            matching_names_df = matching_names_df.append(
                {'Naam school': row_a['Naam school'],
                 'Naam DUO': row_b['NAAM_VOLLEDIG'],
                 'Match': fuzz.partial_ratio(row_a['Naam school'].encode('ISO-8859-1'),
                                             row_b['NAAM_VOLLEDIG'].encode(
                                                 'ISO-8859-1'))}, ignore_index=True)
        if fuzz.partial_ratio(row_a['Naam school'].encode('utf-8'),
                              row_b['NAAM_KORT'].encode('ISO-8859-1')) > 85:
            matching_names_df = matching_names_df.append(
                {'Naam school': row_a['Naam school'], 'Naam DUO': row_b['NAAM_KORT'],
                 'Match': fuzz.partial_ratio(row_a['Naam school'].encode('ISO-8859-1'),
                                             row_b['NAAM_KORT'].encode('ISO-8859-1'))},
                ignore_index=True)
matching_names_df
##

end = time.time()
print('Totale duur fuzzy matching (min): ', ((end - start) / 60))

## beste match selecteren per school.
matching_names_df = matching_names_df.sort_values('Match',
                                                  ascending=False).drop_duplicates(
    ['Naam school'])
matching_names_df = matching_names_df.reset_index(drop=True)
matching_names_df = matching_names_df[['Naam school', 'Naam DUO']]
matching_names_df

## mergen met duo-lijst (naam kort / naam volledig)):
ow_totaal = pd.merge(df_amsterdam_filtered, matching_names_df, left_on='Naam school',
                     right_on='Naam school', how='left')
ow_totaal = ow_totaal.reset_index()

ow_totaal_voll = pd.merge(ow_totaal, ow_instellingen, left_on='Naam DUO',
                          right_on='NAAM_VOLLEDIG', how='left')
ow_totaal_voll = ow_totaal_voll[pd.notnull(ow_totaal_voll['NAAM_VOLLEDIG'])]

ow_totaal_kort = pd.merge(ow_totaal, ow_instellingen, left_on='Naam DUO',
                          right_on='NAAM_KORT', how='left')
ow_totaal_kort = ow_totaal_kort[pd.notnull(ow_totaal_kort['NAAM_KORT'])]

ow_totaal_na = ow_totaal[pd.isnull(ow_totaal['Naam DUO'])]

ow_vollkort = pd.concat([ow_totaal_kort, ow_totaal_voll, ow_totaal_na])
ow_vollkort = ow_vollkort.drop_duplicates(['index'])
ow_vollkort = ow_vollkort.drop_duplicates(keep='first')
ow_vollkort = ow_vollkort.drop(['index'], axis=1)
ow_vollkort
##

sum(pd.isnull(ow_vollkort['Naam DUO']))  # hoeveel vacatures niet gematcht?


##Handmatig adressen invoeren van scholen die niet gekoppeld kunnen worden aan DUO-lijst:
# Kan zijn dat deze lijst moet worden aangevuld.
def set_adres(row):
    if row["Naam school"] == "VOX-klassen":
        return "Buiksloterweg 85"
    elif row["Naam school"] == "Spinoza20first":
        return "Martin Luther Kingpark 1"
    elif row["Naam school"] == "Calvijn College":
        return "Pieter Calandlaan 3"
    elif row["Naam school"] == "DENISE":
        return "Piet Mondriaanstraat 140"
    elif row["Naam school"] == "Cartesius Lyceum":
        return "Frederik Hendrikplnts 7 A"
    elif row["Naam school"] == "Geert Groote College":
        return "Fred. Roeskestraat 84"
    elif row["Naam school"] == "Montessori College Oost":
        return "Fred. Roeskestraat 84"
    elif row["Naam school"] == "ZAAM College van Bestuur & Ondersteuningsbureau":
        return "Dubbelink 2"
    elif row["Naam school"] == "Montessori College Oost":
        return "Polderweg 3"
    elif row["Naam school"] == "De Kleine Nicolaas":
        return "Cornelis Krusemanstraat 10"
    elif row["Naam school"] == "Brede School Annie MG Schmidt":
        return "Pieter Langendijkstraat 44"
    else:
        pass


def set_locatie(row):
    if row["Naam school"] == "VOX-klassen":
        return "POINT(4.9126112999999805 52.3872089)"
    elif row["Naam school"] == "Spinoza20first":
        return "POINT(4.905597899999975 52.3394308)"
    elif row["Naam school"] == "Calvijn College":
        return "POINT(4.83152227765652 52.3560953151682)"
    # elif row["Naam school"] == "de Henricus":
    #     return "POINT(4.82838915379233 52.3834820007503)"
    elif row["Naam school"] == "DENISE":
        return "POINT(4.83743179999999 52.3661584)"
    elif row["Naam school"] == "Cartesius Lyceum":
        return "POINT(4.87655639465376 52.377938377558)"
    # elif row["Naam school"] == "De Ark":
    #    return "POINT(4.87474064949524 52.3266284193245)"
    elif row["Naam school"] == "Geert Groote College":
        return "POINT(4.862804299999993 52.3418558)"
    elif row["Naam school"] == "ZAAM College van Bestuur & Ondersteuningsbureau":
        return "POINT(4.950011700000005 52.32278230000001)"
    # elif row["Naam school"] == "BS Elout":
    #    return "POINT(4.85886463448876 52.3503852250802)"
    elif row["Naam school"] == "Basisschool Al Maes":
        return "POINT(4.895167899999933 52.3702157)"
    elif row["Naam school"] == "Montessori College Oost":
        return "POINT(4.9291648999999325 52.3580576)"
    elif row["Naam school"] == "De Kleine Nicolaas":
        return "POINT(4.863194000000021 52.3513465)"
    elif row["Naam school"] == "Brede School Annie MG Schmidt":
        return "POINT(4.861169199999949 52.360273)"
    else:
        pass


ow_vollkort = ow_vollkort.assign(adres2=ow_vollkort.apply(set_adres, axis=1))
ow_vollkort['adres'] = ow_vollkort['adres'].fillna(ow_vollkort['adres2'])

ow_vollkort = ow_vollkort.assign(locatie2=ow_vollkort.apply(set_locatie, axis=1))
ow_vollkort['locatie'] = ow_vollkort['locatie'].fillna(ow_vollkort['locatie2'])

# handmatige aanpassing voor wellantcollege (2 filialen):
ow_vollkort.loc[(ow_vollkort['Naam school'] == "Wellantcollege Sloten") & (
            ow_vollkort['Plaats'] == "Amsterdam"), 'adres'] = "Jan van Zutphenstraat 60"
ow_vollkort.loc[(ow_vollkort['Naam school'] == "Wellantcollege Sloten") & (
            ow_vollkort['Plaats'] == "Amsterdam"), 'plaats'] = "Amsterdam"
ow_vollkort.loc[(ow_vollkort['Naam school'] == "Wellantcollege Linnaeus vmbo") & (
            ow_vollkort['Plaats'] == "Amsterdam"), 'adres'] = "Jan van Zutphenstraat 60"
ow_vollkort.loc[(ow_vollkort['Naam school'] == "Wellantcollege Linnaeus vmbo") & (
            ow_vollkort['Plaats'] == "Amsterdam"), 'plaats'] = "Amsterdam"
ow_vollkort.loc[(ow_vollkort['Naam school'] == "Wellantcollege Sloten") & (ow_vollkort[
                                                                               'Plaats'] == "Amsterdam"), 'locatie'] = "POINT(4.7953800999999885 52.3497342)"
ow_vollkort.loc[(ow_vollkort[
                     'Naam school'] == "Calandlyceum voor Gymnasium Atheneum Havo Vmbo locatie,  Lumion"), 'adres'] = "Anderlechtlaan 3"
ow_vollkort.loc[(ow_vollkort['Naam school'] == "St Jozefschool") & (
            ow_vollkort['Plaats'] == "Amsterdam"), 'adres'] = "Kalfjeslaan 370"
ow_vollkort.loc[(ow_vollkort['Naam school'] == "St Jozefschool") & (
            ow_vollkort['Plaats'] == "Amsterdam"), 'plaats'] = "Amsterdam"
ow_vollkort.loc[(ow_vollkort['Naam school'] == "Saenredam College") & (
            ow_vollkort['Plaats'] == "Zaandijk"), 'adres'] = "Elvis Presleystraat 1"
ow_vollkort.loc[(ow_vollkort['Naam school'] == "Saenredam College") & (
            ow_vollkort['Plaats'] == "Zaandijk"), 'plaats'] = "Zaandijk"

# welke scholen worden qua plaatsnaam niet goed gematcht?
ow_vollkort['inconsistent'] = np.where(((ow_vollkort['Plaats'] != ow_vollkort[
    'plaats']) & ow_vollkort['plaats'].notnull()), 1, 0)

# Het studielokaal verwijderen; Dit is geen school maar een huiswerkbegeleider.
ow_vollkort = ow_vollkort[ow_vollkort["Naam school"] != "Het Studielokaal"]

##Tijdelijke variabelen verwijderen
droplist = ['adres2', 'locatie2', 'inconsistent']
ow_vollkort = ow_vollkort.drop(droplist, axis=1)

backup2 = ow_vollkort

## hercoderen van variabele FTE naar categorieen:
ow_vollkort = ow_vollkort.reset_index()
p = '(\d{1,4}[\,\.]?\d{0,4})'
ow_vollkort_regex = (ow_vollkort['FTE'].str.extractall(p)
                     .unstack('match').fillna(np.NAN)
                     .rename(columns={0: 're1', 1: 're2', 2: 're3', 3: 're4'}))
ow_vollkort_regex.columns = ow_vollkort_regex.columns.droplevel(level=0)
# concat back onto df
ow_vollkort = pd.concat([ow_vollkort, ow_vollkort_regex], axis=1)

if pd.Series(['re1', 're2', 're3', 're4']).isin(ow_vollkort.columns).all():
    regex_cols = ['re1', 're2', 're3', 're4']
elif pd.Series(['re1', 're2', 're3']).isin(ow_vollkort.columns).all():
    regex_cols = ['re1', 're2', 're3']
elif pd.Series(['re1', 're2']).isin(ow_vollkort.columns).all():
    regex_cols = ['re1', 're2']
elif pd.Series(['re1']).isin(ow_vollkort.columns).all():
    regex_cols = ['re1']
else:
    print('ga terug naar script, aanpassing nodig')

for col in regex_cols:
    ow_vollkort[col] = ow_vollkort[col].str.replace(",", ".").astype(float)

# werkt niet als kolom 're2' niet bestaat:
ow_vollkort.loc[
    (ow_vollkort["re1"].notnull() & (ow_vollkort["re2"].isnull())), 'FTE_cat'] = \
ow_vollkort["re1"]
ow_vollkort.loc[
    (ow_vollkort["re1"].notnull() & (ow_vollkort["re2"].notnull())), 'FTE_cat'] = (
            ((ow_vollkort["re2"] - ow_vollkort["re1"]) / 2) + ow_vollkort["re1"])

"""
if (('re2' in ow_vollkort.columns and ow_vollkort[ 're2'].notnull()) and ow_vollkort['re1'].notnull()):
    ow_vollkort['FTE_cat'] = ow_vollkort['re1']
elif 're2' not in ow_vollkort.columns & ow_vollkort['re1'].notnull():
    ow_vollkort['FTE_cat'] = ow_vollkort['re1']
else:
    "NA"
"""

ow_vollkort.loc[(ow_vollkort['FTE'] == "0,6476of0,5789"), 'FTE_cat'] = 0.6
ow_vollkort.loc[(ow_vollkort['FTE'] == "0..6"), 'FTE_cat'] = 0.6
ow_vollkort.loc[(ow_vollkort['FTE'] == "0,6 (4 dagen)"), 'FTE_cat'] = 0.6
ow_vollkort.loc[(ow_vollkort['FTE'] == "0,8 Wtf"), 'FTE_cat'] = 0.7
ow_vollkort.loc[(ow_vollkort['FTE'] == "1,0 en 0,4 fte"), 'FTE_cat'] = 1.4
ow_vollkort.loc[(ow_vollkort['FTE'] == "2 x 0,6-0,7"), 'FTE_cat'] = 1.4
ow_vollkort.loc[(ow_vollkort['FTE'] == "0,2, 0,4 en 1,0"), 'FTE_cat'] = 1.6
ow_vollkort.loc[(ow_vollkort['FTE'] == "10000"), 'FTE_cat'] = 1.0
ow_vollkort.loc[(ow_vollkort['FTE'] == "0,6 + 0,3"), 'FTE_cat'] = 0.9

del ow_vollkort['index']  # kolom 'index' verwijderen
# Regex kolommen verwijderen.
for col in regex_cols:
    ow_vollkort = ow_vollkort.drop(col, axis=1)

##Gefilterd & gekoppeld bestand wegschrijven als .csv met datum van vandaag in bestandsnaam:
today = dt.datetime.today().strftime('%d%m%Y')
ow_vollkort.to_csv('Meesterbaan_{}.csv'.format(today), index=False, sep=';')
from datetime import datetime

from sqlalchemy import Column, Integer, String, Date, Numeric

from psql import Psql

SCHEMA = 'scrape_' + datetime.now().strftime('%d%m%Y_%H%M')
Base = Psql(SCHEMA).set_dec_base(echo=False)


class Categorie(Base):
    """Create class for table, load columns via declarative base."""

    __tablename__ = 'Categories'

    index = Column(Integer, primary_key=True, autoincrement=True)
    status = Column(String(10))
    categorie = Column(String(50))
    added = Column(Date())
    provincie = Column(String(20))
    cat_url = Column(String(200))

    def __init__(self, name, url, date, status, prov):
        """Initialize Categorie class."""
        self.categorie = name
        self.cat_url = url
        self.added = date
        self.status = status
        self.provincie = prov


class Activity(Base):
    """Create class for table, load columns via declarative base."""

    __tablename__ = 'Activities'

    index = Column(Integer, primary_key=True, autoincrement=True)
    status = Column(String(10))
    titel = Column(String(80))
    added = Column(Date())
    provincie = Column(String(25))
    attrac_url = Column(String(200))
    cat_url = Column(String(200))

    def __init__(self, name, url, date, status, prov, bron):
        """Initialize Activity class."""
        self.titel = name
        self.attrac_url = url
        self.added = date
        self.status = status
        self.provincie = prov
        self.cat_url = bron


class Attractie(Base):
    """Create class for table, load columns via declarative base."""

    __tablename__ = 'Attracties'

    index = Column(Integer, primary_key=True, autoincrement=True)
    tripadvisor_id = Column(Integer)
    titel = Column(String(100))
    status = Column(String(25))
    adres = Column(String(100))
    pc_stad = Column(String(100))
    postcode = Column(String(10))
    plaats = Column(String(100))
    land = Column(String(100))
    lat = Column(Numeric())
    lon = Column(Numeric())
    telefoon = Column(String(20))
    beoordeling = Column(Numeric())
    aantal_reviews = Column(Integer)
    pct_excellent = Column(Integer)
    pct_verygood = Column(Integer)
    pct_average = Column(Integer)
    pct_poor = Column(Integer)
    pct_terrible = Column(Integer)
    attrac_url = Column(String(200))

    def __init__(self,
                 status,
                 titel,
                 url,
                 ta_id,
                 rating,
                 straat,
                 pc_stad,
                 postcode,
                 plaats,
                 land,
                 reviews,
                 pct_exc,
                 pct_vg,
                 pct_avg,
                 pct_poor,
                 pct_terr,
                 phone,
                 lat,
                 lon):
        """Initialize Attractie class."""
        self.attrac_url = url
        self.titel = titel
        self.tripadvisor_id = ta_id
        self.beoordeling = rating
        self.adres = straat
        self.pc_stad = pc_stad
        self.postcode = postcode
        self.plaats = plaats
        self.land = land
        self.lat = lat
        self.lon = lon
        self.telefoon = phone
        self.status = status
        self.aantal_reviews = reviews
        self.pct_excellent = pct_exc
        self.pct_verygood = pct_vg
        self.pct_average = pct_avg
        self.pct_poor = pct_poor
        self.pct_terrible = pct_terr


Base.metadata.drop_all(checkfirst=True)
Base.metadata.create_all(checkfirst=True)

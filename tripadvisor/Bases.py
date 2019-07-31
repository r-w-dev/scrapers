from psql import Psql
from sqlalchemy import Column, Integer, String, Date, ForeignKey, Float
from sqlalchemy.orm import relationship


Base = Psql().set_dec_base(schema='scrape4', echo=True)


class Categorie(Base):
    """Create class for table, load columns via declarative base"""

    __tablename__ = 'Categories'

    # id = Column(Integer, primary_key=True, autoincrement=True)
    status = Column(String(10))
    categorie = Column(String(50))
    aantal = Column(Integer)
    added = Column(Date())
    provincie = Column(String(20))
    url = Column(String(200), primary_key=True)
    attributes = relationship("Attribute", lazy="joined", innerjoin=True,
                              back_populates='categorie')

    def __init__(self, name, aantal, url, date, status, prov):
        self.categorie = name
        self.aantal = aantal
        self.url = url
        self.added = date
        self.status = status
        self.provincie = prov


class Attribute(Base):
    """Create class for table, load columns via declarative base"""

    __tablename__ = 'Attributes'

    status = Column(String(10))
    titel = Column(String(80))
    added = Column(Date())
    provincie = Column(String(25))
    url = Column(String(200), primary_key=True)
    bron_categorie_url = Column(String(200), ForeignKey("Categories.url"))
    categorie = relationship("Categorie", back_populates="attributes")

    def __init__(self, name, url, date, status, prov, bron):
        self.titel = name
        self.url = url
        self.added = date
        self.status = status
        self.provincie = prov
        self.bron_categorie_url = bron


class Attractie(Base):

    __tablename__ = 'Attracties'

    status = Column(String(25))
    url = Column(String(200), primary_key=True)
    tripadvisor_id = Column(Integer)
    rating_bubbles = Column(String(25))
    adres_straat_nr = Column(String(100))
    adres_pc_stad = Column(String(100))
    adres_land = Column(String(100))
    latitude = Column(Float)
    longitude = Column(Float)
    phone_nr = Column(String(15))
    reviews = Column(Integer)
    pct_excellent = Column(Integer)
    pct_verygood = Column(Integer)
    pct_average = Column(Integer)
    pct_poor = Column(Integer)
    pct_terrible = Column(Integer)

    def __init__(self, status, url, ta_id, rating, straat, stad, land, reviews,
                 pct_exc, pct_vg, pct_avg, pct_poor, pct_terr, phone, lat, lon):
        self.url = url
        self.tripadvisor_id = ta_id
        self.rating_bubbles = rating
        self.adres_straat_nr = straat
        self.adres_pc_stad = stad
        self.adres_land = land
        self.latitude = lat
        self.longitude = lon
        self.phone_nr = phone
        self.status = status
        self.reviews = reviews
        self.pct_excellent = pct_exc
        self.pct_verygood = pct_vg
        self.pct_average = pct_avg
        self.pct_poor = pct_poor
        self.pct_terrible = pct_terr


Base.metadata.drop_all()
Base.metadata.create_all(checkfirst=True)

from sqlalchemy import create_engine, Column, Integer, String, DateTime, Float, Text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


class Coins(Base):
    __tablename__ = 'tb_coins'  # if you use base it is obrigatory
    id = Column(Integer, primary_key=True)
    name = Column(String)
    symbol = Column(String)
    date_added = Column(Text)
    last_updated = Column(Text)
    price = Column(Float)
    volume_24h = Column(Float)
    circulating_supply = Column(Float)
    total_supply = Column(Float)
    max_supply = Column(Float)
    percent_change_1h = Column(Float)
    percent_change_24h = Column(Float)
    percent_change_7d = Column(Float)

    def start():
        db_string = ""
        engine = create_engine(db_string)
        Session = sessionmaker(bind=engine)
        session = Session()
        Base.metada.create_all(engine)
        print("\nTable created on database")

        return session, engine


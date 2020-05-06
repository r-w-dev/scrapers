from typing import Optional

import pandas as pd

pc4 = pd.read_excel('pc4gem2015.xls', dtype=str)

mra = pd.read_excel(
    'mra regio CBS code 2016.xls',
    engine='xlrd',
    dtype=str,
    usecols=[0, 1, 2, 3],
    names=['GEMEENTE', 'PROVINCIE', 'CBSCODE', 'DEELGEBIED']
)

mra_pc4 = pc4.merge(mra, how='left', left_on='gemeentecode cbs', right_on='CBSCODE', validate='m:1')
mra_pc4 = mra_pc4.loc[mra_pc4.GEMEENTE.notna()]


def find_mra_gemeente(postcode: str = None, plaats: str = None, ret: list = None) -> Optional[str]:
    """
    Vind bijbehorende gemeentecode op basis van postcode (4 / 6) of plaats.

    postcode:
        postcode4 of postcode6

    plaats:
        plaats

    ret:
        1 of meerdere van ['GEMEENTE', 'PROVINCIE', 'CBSCODE', 'DEELGEBIED']

    """
    if postcode is None and plaats is None:
        raise ValueError("Geef postcode of plaats op.")

    res = ''

    if ret:
        if isinstance(ret, list):
            ret = ret[0] if len(ret) == 1 else ret
    else:
        ret = 'CBSCODE'

    postcode = ''.join(filter(str.isalnum, str(postcode)))

    if pd.notna(plaats) and plaats:
        plaats = ''.join(filter(str.strip, str(plaats)))

    try:
        res = mra_pc4.loc[mra_pc4.pc4 == postcode[:4], ret].iloc[0]

    except (IndexError, KeyError):

        if pd.notna(plaats) and plaats:
            try:
                res = mra_pc4.loc[mra_pc4['naam kern'].str.lower() == plaats.lower(), ret].iloc[0]
            except (IndexError, KeyError):
                pass

    if isinstance(res, pd.Series):
        return res if not res.empty else pd.NA

    return res if res else pd.NA

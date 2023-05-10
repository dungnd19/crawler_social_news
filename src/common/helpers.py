from random import choice
from src.common.enums import UserType
from src.common.mdb import db_user


def get_creator():
    users = list(db_user.find({'type': UserType.BOT.value}))
    return choice(users)

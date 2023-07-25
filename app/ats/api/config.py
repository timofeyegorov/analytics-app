import hashlib


def create_config(from_data: str, to_data: str):
    # Шаблон по доке api sipuni
    user = '051340'
    from_date = from_data
    to_date = to_data
    type = '2'
    state = '0'
    tree = '000-478278'
    showTreeId = '0'
    fromNumber = ''
    toNumber = ''
    numbersRinged = 1
    numbersInvolved = 1
    names = 1
    outgoingLine = 1
    toAnswer = ''
    anonymous = '1'
    firstTime = '0'
    dtmfUserAnswer = '0'
    hash = '0.njnaxjoq79'

    # Собираем str-хэш
    hash_string = '+'.join(
        [anonymous, str(dtmfUserAnswer), firstTime, from_date, fromNumber, str(names), str(numbersInvolved),
         str(numbersRinged), str(outgoingLine), showTreeId, state, to_date, toAnswer, toNumber, tree, type, user, hash]
    )

    # получаем сам хэш
    hash = hashlib.md5(hash_string.encode()).hexdigest()

    # собираем параметры для запроса
    data = {
        'anonymous': anonymous,
        'dtmfUserAnswer': dtmfUserAnswer,
        'firstTime': firstTime,
        'from': from_date,
        'fromNumber': fromNumber,
        'names': names,
        'numbersInvolved': numbersInvolved,
        'numbersRinged': numbersRinged,
        'outgoingLine': outgoingLine,
        'showTreeId': showTreeId,
        'state': state,
        'to': to_date,
        'toAnswer': toAnswer,
        'toNumber': toNumber,
        'tree': tree,
        'type': type,
        'user': user,
        'hash': hash,
    }
    return data

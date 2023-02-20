import csv
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from copy import deepcopy

data = []
grouped_data = []
remove_operations = ('Transfer from Main Account/Futures to Margin Account',
                     'Transfer from Margin Account to Main Account/Futures',
                     'Main and Funding Account Transfer',
                     'Simple Earn Flexible Subscription',
                     'Simple Earn Flexible Redemption',
                     'Fiat Deposit',
                     'Small Assets Exchange BNB')
operations = set()
savings = {}
usd_rates = {}

# calc_type = 'FIFO'
calc_type = 'AVG'
# calc_type = 'LIFO'


def get_eur_amount_for_usd(amount, date, coin):
    if 'USD' in coin:
        cur_date = date
        i = 0
        while cur_date.strftime("%Y-%m-%d") not in usd_rates and i < 5:
            cur_date = cur_date - timedelta(days=1)
            i += 1
        date_str = cur_date.strftime("%Y-%m-%d")
        if date_str not in usd_rates:
            raise Exception(f'No {date_str} in usd_rates')
        return amount * usd_rates[date_str], usd_rates[date_str]
    raise NotImplementedError


def get_data_operation(data_row):
    return data_row['Operation']


def get_data_coin(data_row):
    return data_row['Coin']


def get_data_amount(data_row):
    return float(data_row['Change'])


def get_data_date(data_row):
    return data_row['EST_Time']


def add_coin_amount(coin, amount, eur_amount):
    if coin not in savings:
        savings[coin] = {}
        savings[coin]['Sum'] = 0
        savings[coin]['Eur'] = 0
        savings[coin]['List'] = []
    savings[coin]['Sum'] += amount
    savings[coin]['Eur'] += eur_amount
    savings[coin]['List'].append([amount, eur_amount, 1.0 * eur_amount / amount])
    if savings[coin]['Sum'] < -1e-5:
        raise Exception(f'savings[{coin}][Sum] < 0')
    if savings[coin]['Eur'] < -1e-5:
        raise Exception(f'savings[{coin}][Eur] < 0')


def read_file(filename):
    with open(filename, "r") as f:
        file_reader = csv.reader(f, delimiter=';')
        header_names = []
        fee_rows = []
        for row in file_reader:
            if len(header_names) == 0:
                header_names = row
            else:
                data_item = {}
                for i in range(len(header_names)):
                    data_item[header_names[i]] = row[i]
                if data_item['Operation'] not in remove_operations:
                    if data_item['Operation'] == 'Fee' and data[-1]['UTC_Time'] != data_item['UTC_Time']:
                        fee_rows.append(data_item)
                    else:
                        data.append(data_item)
                        if len(fee_rows) > 0:
                            data.extend(fee_rows)
                            fee_rows = []

        if len(fee_rows) > 0:
            data.extend(fee_rows)


def enrich_data():
    for data_item in data:
        utc = ZoneInfo('UTC')
        localtz = ZoneInfo('Europe/Tallinn')
        data_item['UTC_Time'] = datetime.strptime(data_item['UTC_Time'], '%Y-%m-%d %H:%M:%S').replace(tzinfo=utc)
        data_item['EST_Time'] = data_item['UTC_Time'].astimezone(localtz)
        operations.add(data_item['Operation'])

        if len(grouped_data) != 0 and \
                grouped_data[-1]['EST_Time'] == data_item['EST_Time'] and \
                (grouped_data[-1]['Operation'] == data_item['Operation'] or data_item['Operation'] == 'Fee'):
            if get_data_amount(data_item) >= 0 or data_item['Operation'] == 'Fee':
                if 'Plus' not in grouped_data[-1]:
                    grouped_data[-1]['Plus'] = {'Coin': data_item['Coin'], 'Change': get_data_amount(data_item)}
                elif grouped_data[-1]['Plus']['Coin'] == get_data_coin(data_item):
                    grouped_data[-1]['Plus']['Change'] += get_data_amount(data_item)
                else:
                    raise Exception(f"Error while adding row {data_item} to grouped_data[-1]['Plus']")
            else:
                if 'Minus' not in grouped_data[-1]:
                    grouped_data[-1]['Minus'] = {'Coin': data_item['Coin'], 'Change': get_data_amount(data_item)}
                elif grouped_data[-1]['Minus']['Coin'] == get_data_coin(data_item):
                    grouped_data[-1]['Minus']['Change'] += get_data_amount(data_item)
                else:
                    raise Exception(f"Error while adding row {data_item} to grouped_data[-1]['Minus']")
        else:
            grouped_data.append({})
            grouped_data[-1]['EST_Time'] = data_item['EST_Time']
            grouped_data[-1]['Operation'] = data_item['Operation']
            if get_data_amount(data_item) >= 0:
                grouped_data[-1]['Plus'] = {'Coin': data_item['Coin'], 'Change': get_data_amount(data_item)}
            else:
                grouped_data[-1]['Minus'] = {'Coin': data_item['Coin'], 'Change': get_data_amount(data_item)}


def calculate_new_eur_amount(grouped_item):
    if 'Minus' in grouped_item and 'USD' in grouped_item['Minus']['Coin']:
        amount = abs(get_data_amount(grouped_item['Minus']))
        new_eur_amount, rate = get_eur_amount_for_usd(amount, get_data_date(grouped_item),
                                                get_data_coin(grouped_item['Minus']))
    elif 'USD' in grouped_item['Plus']['Coin']:
        amount = get_data_amount(grouped_item['Plus'])
        new_eur_amount, rate = get_eur_amount_for_usd(amount, get_data_date(grouped_item),
                                                get_data_coin(grouped_item['Plus']))
    else:
        raise Exception(f'Not supported operation {grouped_item}')
    return new_eur_amount, rate


def process_minus_avg(coin, amount):
    coin_savings = savings[coin]
    list_savings = coin_savings['List']
    new_rate = 1.0 * coin_savings['Eur'] / coin_savings['Sum']
    for item in list_savings:
        item[2] = new_rate
        item[1] = 1.0 * item[0] * new_rate
    return process_minus_fifo(coin, amount)


def process_minus_fifo(coin, amount):
    coin_savings = savings[coin]
    list_savings = coin_savings['List']
    minus_eur = 0
    minus_eur_list = []
    for i in range(len(list_savings)):
        if amount < -1e-6:
            raise Exception('Problem while minus')
        item = list_savings[i]
        if abs(item[0] - amount) < 1e-6:
            minus_eur += item[1]
            minus_eur_list.append(item)
            coin_savings['Eur'] -= item[1]
            coin_savings['Sum'] -= item[0]
            coin_savings['List'] = list_savings[i + 1:]
            amount -= item[0]
            break
        elif item[0] > amount:
            item[0] -= amount
            item[1] -= amount * item[2]
            minus_eur += amount * item[2]
            minus_eur_list.append((amount, amount * item[2], item[2]))
            coin_savings['Eur'] -= amount * item[2]
            coin_savings['Sum'] -= amount
            coin_savings['List'] = list_savings[i:]
            amount -= amount
            break
        else:
            minus_eur += item[1]
            minus_eur_list.append(item)
            coin_savings['Eur'] -= item[1]
            coin_savings['Sum'] -= item[0]
            amount -= item[0]
    if abs(amount) > 1e-6:
        raise Exception(f'Not enough coins {coin}')
    return minus_eur, minus_eur_list


def process_minus_lifo(coin, amount):
    coin_savings = savings[coin]
    list_savings = coin_savings['List']
    minus_eur = 0
    minus_eur_list = []
    for i in range(len(list_savings) - 1, -1, -1):
        if amount < -1e-6:
            raise Exception('Problem while minus')
        item = list_savings[i]
        if abs(item[0] - amount) < 1e-6:
            minus_eur += item[1]
            minus_eur_list.append(item)
            coin_savings['Eur'] -= item[1]
            coin_savings['Sum'] -= item[0]
            coin_savings['List'] = list_savings[:i]
            amount -= item[0]
            break
        elif item[0] > amount:
            item[0] -= amount
            item[1] -= amount * item[2]
            minus_eur += amount * item[2]
            minus_eur_list.append((amount, amount * item[2], item[2]))
            coin_savings['Eur'] -= amount * item[2]
            coin_savings['Sum'] -= amount
            coin_savings['List'] = list_savings[:i + 1]
            amount -= amount
            break
        else:
            minus_eur += item[1]
            minus_eur_list.append(item)
            coin_savings['Eur'] -= item[1]
            coin_savings['Sum'] -= item[0]
            amount -= item[0]
    if abs(amount) > 1e-6:
        raise Exception(f'Not enough coins {coin}')
    return minus_eur


def process_minus_coin(calc_type, coin, amount):
    if calc_type == 'AVG':
        return process_minus_avg(coin, amount)
    elif calc_type == 'FIFO':
        return process_minus_fifo(coin, amount)
    elif calc_type == 'LIFO':
        return process_minus_lifo(coin, amount)
    else:
        raise Exception(f'calc type {calc_type} is not supported')


def process_grouped_data(calc_type):
    for grouped_item in grouped_data:
        new_eur_amount, eur_usd = calculate_new_eur_amount(grouped_item)
        grouped_item['eur_usd'] = eur_usd

        if 'Minus' in grouped_item:
            amount_minus = get_data_amount(grouped_item['Minus'])
            coin_minus = get_data_coin(grouped_item['Minus'])
            amount_plus = get_data_amount(grouped_item['Plus'])
            coin_plus = get_data_coin(grouped_item['Plus'])

            old_eur_amount, old_eur_list = process_minus_coin(calc_type, coin_minus, abs(amount_minus))
            add_coin_amount(coin_plus, amount_plus, new_eur_amount)

            grouped_item['new_eur_amount'] = new_eur_amount
            grouped_item['old_eur_amount'] = old_eur_amount
            grouped_item['profit'] = new_eur_amount - old_eur_amount
            grouped_item['minus_coin_list'] = old_eur_list
            if abs(new_eur_amount - old_eur_amount) < 1e-6 or old_eur_amount > new_eur_amount:
                grouped_item['profit_flag'] = False
            else:
                grouped_item['profit_flag'] = True
        else:
            amount_plus = get_data_amount(grouped_item['Plus'])
            coin_plus = get_data_coin(grouped_item['Plus'])
            add_coin_amount(coin_plus, amount_plus, new_eur_amount)

            grouped_item['new_eur_amount'] = new_eur_amount
        grouped_item['savings'] = deepcopy(savings)


def show_operations(result_filename):
    total_profit = 0
    plus_profit = 0
    with open(result_filename, 'w') as f:
        fieldnames = ['Time', 'Minus coin', 'Plus coin', 'Minus amount', 'Plus amount', 'Is profit?', 'Profit',
                      'old_eur_amount', 'new_eur_amount', 'EUR/USD', 'BTC', 'USDT']
        file_writer = csv.writer(f, delimiter=';', lineterminator='\n')
        file_writer.writerow(fieldnames)

        for grouped_item in grouped_data:
            file_writer.writerow([grouped_item['EST_Time'],
                                  get_data_coin(grouped_item["Minus"]) if 'Minus' in grouped_item else None,
                                  get_data_coin(grouped_item["Plus"]),
                                  get_data_amount(grouped_item["Minus"]) if 'Minus' in grouped_item else None,
                                  get_data_amount(grouped_item["Plus"]),
                                  grouped_item.get('profit_flag'),
                                  grouped_item.get('profit'),
                                  grouped_item.get('old_eur_amount'),
                                  grouped_item.get('new_eur_amount'),
                                  grouped_item['eur_usd'],
                                  grouped_item.get('savings').get('BTC', {}).get('Sum'),
                                  grouped_item.get('savings').get('USDT', {}).get('Sum')
                                  ])
            print(grouped_item['EST_Time'], grouped_item['Operation'], grouped_item.get('Plus', 'No Plus'),
                  grouped_item.get('Minus', 'No minus'))
            if 'profit_flag' not in grouped_item:
                print(f'Add coin {get_data_coin(grouped_item["Plus"])} amount={get_data_amount(grouped_item["Plus"])} '
                      f'eur_amount={grouped_item["new_eur_amount"]}')
            else:
                total_profit += grouped_item["profit"]
                if grouped_item['profit_flag']:
                    plus_profit += grouped_item["profit"]

                print(
                    f'Profit={grouped_item["profit_flag"]}, old_eur_amount={grouped_item["old_eur_amount"]}, '
                    f'new_eur_amount={grouped_item["new_eur_amount"]}, profit={grouped_item["profit"]}, '
                    f'coins={grouped_item["minus_coin_list"]}')
                print(f'{get_data_coin(grouped_item["Minus"])}: '
                      f'{grouped_item["savings"][get_data_coin(grouped_item["Minus"])]}')
            print("----------")

        print(f'Total profit: {total_profit}')
        print(f'Plus profit: {plus_profit}')


def load_usd_rates(filename):
    with open(filename, "r") as f:
        file_reader = csv.reader(f, delimiter=',')
        for row in file_reader:
            usd_rates[row[0]] = float(row[1])


# ['User_ID', 'UTC_Time', 'Account', 'Operation', 'Coin', 'Change', 'Remark']
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    read_file("tax_input.csv")
    load_usd_rates("eur_usd_rate_2022.csv")
    enrich_data()
    process_grouped_data(calc_type)
    show_operations(f"results_{calc_type}.csv")

# See PyCharm help at https://www.jetbrains.com/help/pycharm/

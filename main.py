import csv
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


@dataclass
class CoinExchangeItem:
    amount: float
    eur_amount: float
    exchange_eur_rate: float


@dataclass
class CoinSaving:
    sum: float = 0
    sum_eur: float = 0
    coins_list: list[CoinExchangeItem] = field(default_factory=list)


@dataclass
class Transaction:
    coin: str
    amount: float


@dataclass
class Operation:
    est_time: datetime = None
    utc_time: datetime = None
    type: str = None
    plus: Transaction = None
    minus: Transaction = None
    eur_usd_rate: float = None
    new_eur_amount: float = None
    old_eur_amount: float = None
    profit_flag: bool = None
    profit: float = None
    coins_list: list[CoinExchangeItem] = None
    savings_after: dict[str, CoinSaving] = field(default_factory=dict)


remove_operations = ('Transfer from Main Account/Futures to Margin Account',
                     'Transfer from Margin Account to Main Account/Futures',
                     'Main and Funding Account Transfer',
                     'Simple Earn Flexible Subscription',
                     'Simple Earn Flexible Redemption',
                     'Fiat Deposit',
                     'Small Assets Exchange BNB')
EPS: float = 1e-6
savings: dict[str, CoinSaving] = {}
operations: list[Operation] = []
usd_rates = {}

# calc_type = 'FIFO'
calc_type = 'AVG'


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


def get_data_amount(data_row):
    return float(data_row['Change'])


def add_coin_amount(coin, amount, eur_amount):
    if coin not in savings:
        savings[coin] = CoinSaving()
    savings[coin].sum += amount
    savings[coin].sum_eur += eur_amount
    savings[coin].coins_list.append(CoinExchangeItem(amount, eur_amount, 1.0 * eur_amount / amount))
    if savings[coin].sum < -EPS:
        raise Exception(f'savings[{coin}].sum < 0')
    if savings[coin].sum_eur < -EPS:
        raise Exception(f'savings[{coin}].sum_eur < 0')


# csv columns: ['User_ID', 'UTC_Time', 'Account', 'Operation', 'Coin', 'Change', 'Remark']
def read_file(filename):
    data = []
    with open(filename, "r") as f:
        file_reader = csv.reader(f, delimiter=';')
        header_names = []
        for row in file_reader:
            if len(header_names) == 0:
                header_names = row
            else:
                data_item = {}
                for i in range(len(header_names)):
                    data_item[header_names[i]] = row[i]
                if data_item['Operation'] not in remove_operations:
                    data.append(data_item)
    return data


def enrich_data(read_data):
    for read_data_item in read_data:
        utc = ZoneInfo('UTC')
        localtz = ZoneInfo('Europe/Tallinn')
        read_data_item['UTC_Time'] = datetime.strptime(read_data_item['UTC_Time'], '%Y-%m-%d %H:%M:%S').replace(
            tzinfo=utc)
        read_data_item['EST_Time'] = read_data_item['UTC_Time'].astimezone(localtz)

        if len(operations) != 0 and \
                operations[-1].est_time == read_data_item['EST_Time'] and \
                (operations[-1].type == read_data_item['Operation'] or read_data_item['Operation'] == 'Fee' or
                 operations[-1].type == 'Fee'):
            if read_data_item['Operation'] != 'Fee':
                operations[-1].type = read_data_item['Operation']
            if float(read_data_item['Change']) >= 0 or read_data_item['Operation'] == 'Fee':
                if operations[-1].plus is None:
                    operations[-1].plus = Transaction(coin=read_data_item['Coin'],
                                                      amount=float(read_data_item['Change']))
                elif operations[-1].plus.coin == read_data_item['Coin']:
                    operations[-1].plus.amount += float(read_data_item['Change'])
                else:
                    raise Exception(f"Error while adding row {read_data_item} to grouped_data[-1].plus")
            else:
                if operations[-1].minus is None:
                    operations[-1].minus = Transaction(coin=read_data_item['Coin'],
                                                       amount=float(read_data_item['Change']))
                elif operations[-1].minus.coin == read_data_item['Coin']:
                    operations[-1].minus.amount += float(read_data_item['Change'])
                else:
                    raise Exception(f"Error while adding row {read_data_item} to grouped_data[-1].minus")
        else:
            operations.append(Operation())
            operations[-1].est_time = read_data_item['EST_Time']
            operations[-1].type = read_data_item['Operation']
            if float(read_data_item['Change']) >= 0 or read_data_item['Operation'] == 'Fee':
                operations[-1].plus = Transaction(coin=read_data_item['Coin'],
                                                  amount=float(read_data_item['Change']))
            else:
                operations[-1].minus = Transaction(coin=read_data_item['Coin'],
                                                   amount=float(read_data_item['Change']))


def calculate_new_eur_amount(operation):
    if operation.minus is not None and 'USD' in operation.minus.coin:
        new_eur_amount, rate = get_eur_amount_for_usd(abs(operation.minus.amount), operation.est_time,
                                                      operation.minus.coin)
    elif 'USD' in operation.plus.coin:
        new_eur_amount, rate = get_eur_amount_for_usd(operation.plus.amount, operation.est_time, operation.plus.coin)
    else:
        raise Exception(f'Not supported operation {operation}')
    return new_eur_amount, rate


def process_minus_avg(coin, amount):
    coin_savings = savings[coin]
    coin_saving_list = coin_savings.coins_list
    new_rate = 1.0 * coin_savings.sum_eur / coin_savings.sum
    for coin_saving_item in coin_saving_list:
        coin_saving_item.exchange_eur_rate = new_rate
        coin_saving_item.eur_amount = 1.0 * coin_saving_item.amount * new_rate
    return process_minus_fifo(coin, amount)


def process_minus_fifo(coin, minus_amount) -> (float, list[CoinExchangeItem]):
    coin_savings = savings[coin]
    coin_saving_list = coin_savings.coins_list
    minus_eur = 0
    minus_eur_list: list[CoinExchangeItem] = []
    for i in range(len(coin_saving_list)):
        if minus_amount < -EPS:
            raise Exception(f'Problem while minus operation. There is not enough coins {coin}')
        coin_saving_item = coin_saving_list[i]
        if abs(coin_saving_item.amount - minus_amount) < EPS:
            minus_eur += coin_saving_item.eur_amount
            minus_eur_list.append(coin_saving_item)
            coin_savings.sum_eur -= coin_saving_item.eur_amount
            coin_savings.sum -= coin_saving_item.amount
            coin_savings.coins_list = coin_saving_list[i + 1:]
            minus_amount -= coin_saving_item.amount
            break
        elif coin_saving_item.amount > minus_amount:
            coin_saving_item.amount -= minus_amount
            coin_saving_item.eur_amount -= minus_amount * coin_saving_item.exchange_eur_rate
            minus_eur += minus_amount * coin_saving_item.exchange_eur_rate
            minus_eur_list.append(CoinExchangeItem(minus_amount,
                                                   minus_amount * coin_saving_item.exchange_eur_rate,
                                                   coin_saving_item.exchange_eur_rate))
            coin_savings.sum -= minus_amount
            coin_savings.sum_eur -= minus_amount * coin_saving_item.exchange_eur_rate
            coin_savings.coins_list = coin_saving_list[i:]
            minus_amount -= minus_amount
            break
        else:
            minus_eur += coin_saving_item.eur_amount
            minus_eur_list.append(coin_saving_item)
            coin_savings.sum_eur -= coin_saving_item.eur_amount
            coin_savings.sum -= coin_saving_item.amount
            minus_amount -= coin_saving_item.amount
    if abs(minus_amount) > EPS:
        raise Exception(f'Not enough coins {coin}')
    return minus_eur, minus_eur_list


def process_minus_coin(calc_type, coin, amount) -> (float, list[CoinExchangeItem]):
    if calc_type == 'AVG':
        return process_minus_avg(coin, amount)
    elif calc_type == 'FIFO':
        return process_minus_fifo(coin, amount)
    else:
        raise Exception(f'calc type {calc_type} is not supported')


def process_grouped_data(calc_type):
    for operation in operations:
        operation.new_eur_amount, operation.eur_usd_rate = calculate_new_eur_amount(operation)

        if operation.minus is not None:
            operation.old_eur_amount, operation.coins_list = process_minus_coin(calc_type, operation.minus.coin,
                                                                                abs(operation.minus.amount))
            add_coin_amount(operation.plus.coin, operation.plus.amount, operation.new_eur_amount)
            operation.profit = operation.new_eur_amount - operation.old_eur_amount
            operation.profit_flag = abs(operation.profit) > EPS and operation.profit > 0
        else:
            add_coin_amount(operation.plus.coin, operation.plus.amount, operation.new_eur_amount)
        operation.savings_after = deepcopy(savings)


def show_operations(result_filename):
    total_profit = 0
    plus_profit = 0
    with open(result_filename, 'w') as f:
        fieldnames = ['Time', 'Minus coin', 'Plus coin', 'Minus amount', 'Plus amount', 'Is profit?', 'Profit',
                      'old_eur_amount', 'new_eur_amount', 'EUR/USD', 'BTC', 'USDT', 'exchange_eur_rate', 'Minus source']
        file_writer = csv.writer(f, delimiter=';', lineterminator='\n')
        file_writer.writerow(fieldnames)

        for operation in operations:
            file_writer.writerow([operation.est_time,
                                  operation.minus.coin if operation.minus is not None else None,
                                  operation.plus.coin,
                                  operation.minus.amount if operation.minus is not None else None,
                                  operation.plus.amount,
                                  operation.profit_flag,
                                  operation.profit,
                                  operation.old_eur_amount,
                                  operation.new_eur_amount,
                                  operation.eur_usd_rate,
                                  operation.savings_after.get('BTC', CoinSaving()).sum,
                                  operation.savings_after.get('USDT', CoinSaving()).sum,
                                  operation.coins_list[0].exchange_eur_rate if operation.coins_list is not None and len(operation.coins_list) > 0 else None,
                                  operation.coins_list
                                  ])
            if operation.profit_flag is not None:
                total_profit += operation.profit
                if operation.profit_flag:
                    plus_profit += operation.profit

        print(f'Total profit: {total_profit}')
        print(f'Plus profit: {plus_profit}')


def load_usd_rates(filename):
    with open(filename, "r") as f:
        file_reader = csv.reader(f, delimiter=',')
        for row in file_reader:
            usd_rates[row[0]] = float(row[1])


if __name__ == '__main__':
    load_usd_rates("eur_usd_rate_2022.csv")
    read_data = read_file("tax_input.csv")
    enrich_data(read_data)
    process_grouped_data(calc_type)
    show_operations(f"results_{calc_type}.csv")

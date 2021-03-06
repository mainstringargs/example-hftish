import argparse
import pandas as pd
import numpy as np
import alpaca_trade_api as tradeapi
import datetime
from pylivetrader.errors import (
    SymbolNotFound,
    OrderDuringInitialize,
    TradingControlViolation,
    RegisterTradingControlPostInit,
)
import pylivetrader.protocol as proto
from pylivetrader.misc import events
from pylivetrader.algorithm import Algorithm
from pylivetrader.executor.executor import AlgorithmExecutor
from pylivetrader.misc.api_context import LiveTraderAPI
from pylivetrader.loader import get_functions

from pylivetrader.api import (
    attach_pipeline,
    date_rules,
    get_datetime,
    time_rules,
    order,
    get_open_orders,
    cancel_order,
    pipeline_output,
    schedule_function,
)

from pylivetrader import *


from pylivetrader.misc.events import (
    EventManager,
    make_eventrule,
    date_rules,
    time_rules,
    calendars,
    AfterOpen,
    BeforeClose
)
import os
import threading
import asyncio

class Quote():
    """
    We use Quote objects to represent the bid/ask spread. When we encounter a
    'level change', a move of exactly 1 penny, we may attempt to make one
    trade. Whether or not the trade is successfully filled, we do not submit
    another trade until we see another level change.

    Note: Only moves of 1 penny are considered eligible because larger moves
    could potentially indicate some newsworthy event for the stock, which this
    algorithm is not tuned to trade.
    """

    def __init__(self):
        self.prev_bid = 0
        self.prev_ask = 0
        self.prev_spread = 0
        self.bid = 0
        self.ask = 0
        self.bid_size = 0
        self.ask_size = 0
        self.spread = 0
        self.traded = True
        self.level_ct = 1
        self.time = 0

    def reset(self):
        # Called when a level change happens
        self.traded = False
        self.level_ct += 1

    def update(self, data):
        # Update bid and ask sizes and timestamp
        self.bid_size = data.bidsize
        self.ask_size = data.asksize

        # Check if there has been a level change
        if (
            self.bid != data.bidprice
            and self.ask != data.askprice
            and round(data.askprice - data.bidprice, 2) == .01
        ):
            # Update bids and asks and time of level change
            self.prev_bid = self.bid
            self.prev_ask = self.ask
            self.bid = data.bidprice
            self.ask = data.askprice
            self.time = data.timestamp
            # Update spreads
            self.prev_spread = round(self.prev_ask - self.prev_bid, 3)
            self.spread = round(self.ask - self.bid, 3)
            print(
                str(datetime.datetime.now()),'Level change:', self.prev_bid, self.prev_ask,
                self.prev_spread, self.bid, self.ask, self.spread, flush=True
            )
            # If change is from one penny spread level to a different penny
            # spread level, then initialize for new level (reset stale vars)
            if self.prev_spread == 0.01:
                self.reset()


class Position():
    """
    The position object is used to track how many shares we have. We need to
    keep track of this so our position size doesn't inflate beyond the level
    we're willing to trade with. Because orders may sometimes be partially
    filled, we need to keep track of how many shares are "pending" a buy or
    sell as well as how many have been filled into our account.
    """

    def __init__(self, symbol):
        self.orders_filled_amount = {}
        self.pending_buy_shares = 0
        self.pending_sell_shares = 0
        self.total_shares = 0
        self.symbol = symbol

    def update_pending_buy_shares(self, quantity):
        self.pending_buy_shares += quantity

    def update_pending_sell_shares(self, quantity):
        self.pending_sell_shares += quantity

    def update_filled_amount(self, order_id, new_amount, side):
        try:
            old_amount = self.orders_filled_amount[order_id]
            if new_amount > old_amount:
                if side == 'buy':
                    self.update_pending_buy_shares(old_amount - new_amount)
                    self.update_total_shares(new_amount - old_amount)
                else:
                    self.update_pending_sell_shares(old_amount - new_amount)
                    self.update_total_shares(old_amount - new_amount)
                self.orders_filled_amount[order_id] = new_amount
        except:
            print("update_filled_amount except");                

    def remove_pending_order(self, order_id, side):
        try:
            old_amount = self.orders_filled_amount[order_id]
            if side == 'buy':
                self.update_pending_buy_shares(old_amount - 100)
            else:
                self.update_pending_sell_shares(old_amount - 100)
            del self.orders_filled_amount[order_id]
        except:
            print("remove_pending_order except");

    def update_total_shares(self, quantity):
        self.total_shares += quantity
        print(str(datetime.datetime.now()) + " Total Shares of "+self.symbol +": "+str(self.total_shares));

    def get_total_shares(self):
        return self.total_shares;        

conn = None;
args = None;
api = None;
symbol = None;
max_shares = None;
submitBuys = True;
sessionStartPortfolioValue = None;
stopStream = False;


def initialize(context):
    print("initialize");
    
    global symbol
    global max_shares
    symbol = "TQQQ"
    max_shares = 500
    shouldLiquidate = True;
      
    opts = {}
    opts['key_id'] = os.environ['APCA_API_KEY_ID']
    opts['secret_key'] = os.environ['APCA_API_SECRET_KEY']
    opts['base_url'] = os.environ['APCA_API_BASE_URL']

    print(get_datetime());
    
    global api
    api = tradeapi.REST(**opts)
    
    clock = api.get_clock();
    

    
    schedule_function(
        stop_stream,
        date_rules.every_day(),
        time_rules.market_close(
            hours=0,
            minutes=10),calendars.US_EQUITIES)
            
    print(str(datetime.datetime.now()) + " stop_stream Scheduled");

    schedule_function(
        cancel_open_buy_orders,
        date_rules.every_day(),
        time_rules.market_close(
            hours=0,
            minutes=11),calendars.US_EQUITIES)      

    print(str(datetime.datetime.now()) + " cancel_open_buy_orders Scheduled");            

    schedule_function(
        stop_buying,
        date_rules.every_day(),
        time_rules.market_close(
            hours=0,
            minutes=12),calendars.US_EQUITIES)         


    print(str(datetime.datetime.now()) + " stop_buying Scheduled");            
            
    schedule_function(
        cancel_open_orders,
        date_rules.every_day(),
        time_rules.market_close(
            hours=0,
            minutes=7),calendars.US_EQUITIES)
            
    print(str(datetime.datetime.now()) + " cancel_open_orders Scheduled");
                
            
    if(shouldLiquidate is True):
        print(str(datetime.datetime.now()) + " liquidate Scheduled");
        
        schedule_function(
            liquidate,
            date_rules.every_day(),
            time_rules.market_close(
                hours=0,
                minutes=3),calendars.US_EQUITIES) 
                
    schedule_function(
        end_of_day_dump,
        date_rules.every_day(),
        time_rules.market_close(),calendars.US_EQUITIES)          

    print(str(datetime.datetime.now()) + " end_of_day_dump Scheduled");        

    schedule_function(
        start_stream,
        date_rules.every_day(),
        time_rules.market_open(
            minutes=3),calendars.US_EQUITIES)
    
    print(str(datetime.datetime.now()) + " start_stream Scheduled");
    
    print("Clock "+str(clock));
    
    if(clock.__getattr__("is_open")):
        print("Market is open; starting");
        start_stream(None,None)
    


def run():

    global symbol
    global max_shares
    global opts 
    global submitBuys
    global sessionStartPortfolioValue
    
    submitBuys = True
    
    symbol = symbol.upper()
    quote = Quote()
    qc = 'Q.%s' % symbol
    tc = 'T.%s' % symbol
    
    position = Position(symbol)
    global api
        
    try:
        currPos = api.get_position(symbol);
        
        if(currPos is not None):
            position.update_total_shares(int(currPos.__getattr__("qty")))
    except:
        print("No current position in "+symbol);
        
    print(str(datetime.datetime.now()) + " account value at run $"+api.get_account().__getattr__("portfolio_value"));
    
    sessionStartPortfolioValue = api.get_account().__getattr__("portfolio_value");

    opts = {}
    opts['key_id'] = os.environ['APCA_API_KEY_ID']
    opts['secret_key'] = os.environ['APCA_API_SECRET_KEY']
    opts['base_url'] = os.environ['APCA_API_BASE_URL']

    print(str(opts));
    
    # Establish streaming connection
    conn = tradeapi.StreamConn(**opts)

    # Define our message handling
    @conn.on(r'Q\.' + symbol)
    async def on_quote(conn, channel, data):
        global stopStream;
        
        if(stopStream):
            #myfuture1 = asyncio.ensure_future(conn.close())
            #loop = asyncio.get_event_loop()
            #loop.run_until_complete(myfuture1)
            #loop.close()
            await conn.close();
            return;
    
        # Quote update received
        quote.update(data)

    @conn.on(r'T\.' + symbol)
    async def on_trade(conn, channel, data):
    
        global submitBuys
        global stopStream;
        
        if(stopStream):
            await conn.close();
            return;
    
    
        if quote.traded:
            return
        # We've received a trade and might be ready to follow it
        if (
            data.timestamp <= (
                quote.time + pd.Timedelta(np.timedelta64(50, 'ms'))
            )
        ):
            # The trade came too close to the quote update
            # and may have been for the previous level
            return
        #print(str(datetime.datetime.now()) + " Data size "+str(data.size));
        if data.size >= 100:
            # The trade was large enough to follow, so we check to see if
            # we're ready to trade. We also check to see that the
            # bid vs ask quantities (order book imbalance) indicate
            # a movement in that direction. We also want to be sure that
            # we're not buying or selling more than we should.
            if (
                data.price == quote.ask
                and quote.bid_size > (quote.ask_size * 1.8)
                and (
                    position.total_shares + position.pending_buy_shares
                ) < max_shares - 100
            ):
                # Everything looks right, so we submit our buy at the ask
                try:
                    print(str(datetime.datetime.now()) + ' Attempting Buy @ '+str(quote.ask) + ' submitBuys? '+str(submitBuys));
                    if(submitBuys is True):
                        o = api.submit_order(
                            symbol=symbol, qty='100', side='buy',
                            type='limit', time_in_force='day',
                            limit_price=str(quote.ask)
                        )
                        # Approximate an IOC order by immediately cancelling
                        api.cancel_order(o.id)
                        position.update_pending_buy_shares(100)
                        position.orders_filled_amount[o.id] = 0
                        print('Buy at', quote.ask, flush=True)
                        quote.traded = True
                except Exception as e:
                    print(e)
            elif (
                data.price == quote.bid
                and quote.ask_size > (quote.bid_size * 1.8)
                and (
                    position.total_shares - position.pending_sell_shares
                ) >= 100
            ):
                # Everything looks right, so we submit our sell at the bid
                try:
                    print(str(datetime.datetime.now()) + ' Attempting sell @ '+str(quote.bid) + " "+str(position.total_shares)+ " "+str(position.pending_sell_shares));                
                    o = api.submit_order(
                        symbol=symbol, qty='100', side='sell',
                        type='limit', time_in_force='day',
                        limit_price=str(quote.bid)
                    )
                    # Approximate an IOC order by immediately cancelling
                    api.cancel_order(o.id)
                    position.update_pending_sell_shares(100)
                    position.orders_filled_amount[o.id] = 0
                    print('Sell at', quote.bid, flush=True)
                    quote.traded = True
                except Exception as e:
                    print(e)

    @conn.on(r'trade_updates')
    async def on_trade_updates(conn, channel, data):
        # We got an update on one of the orders we submitted. We need to
        # update our position with the new information.
        event = data.event
        if event == 'fill':
            if data.order['side'] == 'buy':
                position.update_total_shares(
                    int(data.order['filled_qty'])
                )
            else:
                position.update_total_shares(
                    -1 * int(data.order['filled_qty'])
                )
            position.remove_pending_order(
                data.order['id'], data.order['side']
            )
        elif event == 'partial_fill':
            position.update_filled_amount(
                data.order['id'], int(data.order['filled_qty']),
                data.order['side']
            )
        elif event == 'canceled' or event == 'rejected':
            position.remove_pending_order(
                data.order['id'], data.order['side']
            )

    conn.run(
        ['trade_updates', tc, qc]
    )

def cancel_open_orders(context, data):
    global api
    print(str(datetime.datetime.now()) + " Canceling all open orders");

    open_orders = api.list_orders(
        status='open'
    )
    
    print(open_orders)
    
    for oo in open_orders:
        print(api.cancel_order(oo.__getattr__("id")));

def cancel_open_buy_orders(context, data):
    global api
    print(str(datetime.datetime.now()) + " Canceling all open buy orders");

    open_orders = api.list_orders(
        status='open'
    )
    
    for oo in open_orders:
        if "buy"==oo.side:  # it is a buy order
            api.cancel_order(oo.__getattr__("id"));                
            
def liquidate(context, data):
    global api
    print(str(datetime.datetime.now()) + " Liquidating account");
    
        # Get a list of all of our positions.
    portfolio = api.list_positions()

    # Print the quantity of shares for each position.
    for position in portfolio:
        order = api.submit_order(
                        symbol=symbol, qty=str(position.__getattr__("qty")), side='sell',
                        type='market', time_in_force='day'
                    )
        print(str(datetime.datetime.now()) + " Selling "+str(position));
    
def stop_stream(context, data):
    global stopStream;
    print(str(datetime.datetime.now()) +  " Stopping Stream");    
    stopStream = True;
    
def run_thread(context):
    event_loop = asyncio.new_event_loop();
    asyncio.set_event_loop(event_loop)
    run();
    
def start_stream(context, data):
    global stopStream;
    stopStream = False;
    print(str(datetime.datetime.now()) +  " Starting Stream");        
    t = threading.Thread(target=run_thread, args=(context,))
    t.daemon = True
    t.start()

    
def stop_buying(context, data):
    global submitBuys
    print(str(datetime.datetime.now()) +  " Stop buying");        
    submitBuys = False    
    
def end_of_day_dump(context, data):
    global sessionStartPortfolioValue
    global api
    print(str(datetime.datetime.now()) +  " Start of day: $" + sessionStartPortfolioValue +" End of day: $"+api.get_account().__getattr__("portfolio_value"));         

def handle_data(context, data):
    pass
           
           


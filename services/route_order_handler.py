from __future__ import print_function
from __future__ import absolute_import

import os
import platform as plat
import sys
import blpapi

from create_order_handler import CreateOrderHandler

d_service = '//blp/emapisvc_beta'
d_host = 'localhost'
d_port = 8194
b_end = False


SESSION_STARTED         = blpapi.Name("SessionStarted")
SESSION_STARTUP_FAILURE = blpapi.Name("SessionStartupFailure")
SERVICE_OPENED          = blpapi.Name("ServiceOpened")
SERVICE_OPEN_FAILURE    = blpapi.Name("ServiceOpenFailure")
ERROR_INFO              = blpapi.Name("ErrorInfo")

ROUTE_ORDER             = blpapi.Name("RouteEx")


class RouteOrderHandler():
    def __init__(self, data_msg):
        self.request_id = 0
        self._message_route   = {}

        if data_msg != None or data_msg != {}:
            self._request = data_msg['request'] 
            self._response = data_msg['response']
            self._message_route['_id'] = data_msg['_id']
        else:
            self._request = None 
            self._response = None



    def _process_event(self, event, session):
        try:
            if event.eventType() == blpapi.Event.SESSION_STATUS:
                self._process_session_status_event(event, session)
            elif event.eventType() == blpapi.Event.SERVICE_STATUS:
                self._process_service_status_service(event, session)
            elif event.eventType() == blpapi.Event.RESPONSE:
                self._process_response_event(event)
            else:
                self._process_misc_event(event)
        except Exception as e:
            print(f'Catch: {e}')

        return False
        

    def _process_session_status_event(self, event, session):
        print('Processing SESSION_STATUS event')

        for msg in event:
            if msg.messageType() == SESSION_STARTED:
                print(f'Bloomberg Session Started...\nService: {d_service}')
                session.openServiceAsync(d_service)

            elif msg.messageType() == SESSION_STARTUP_FAILURE:
                print(f'Bloomber Session Startup Failure: {sys.stderr}')

            else:
                print(msg)


    def _process_service_status_service(self, event, session):
        print('Processing SERVICE_STATUS event')

        for msg in event:

            if msg.messageType() == SERVICE_OPENED:
                print('Service Opened...')

                service = session.getService(d_service)
                request = service.createRequest("RouteEx")

                if self._response != None or self._response != {}:
                    
                    params = {}
                    req = {}

                    for i in self._response['CreateOrder']:
                        params[i] = self._response['CreateOrder'][i]

                    for i in self._request['CreateOrder']:
                        req[i] = self._request['CreateOrder'][i]

                    print(f'{params}\n{req}')

                    request.set('EMSX_SEQUENCE', params['EMSX_SEQUENCE'])
                    request.set('EMSX_AMOUNT', req['EMSX_AMOUNT'])
                    request.set('EMSX_BROKER', req['EMSX_BROKER'])
                    request.set('EMSX_HAND_INSTRUCTION', 'ANY')
                    request.set('EMSX_ORDER_TYPE', req['EMSX_ORDER_TYPE'])
                    request.set('EMSX_TICKER', req['EMSX_TICKER'])
                    request.set('EMSX_TIF', req['EMSX_TIF'])
                    request.set('EMSX_NOTES', f'Roteamento de ordens do cliente {params["EMSX_SEQUENCE"]}')

                    strategy = request.getElement("EMSX_STRATEGY_PARAMS")
                    strategy.setElement("EMSX_STRATEGY_NAME", "CASH")
                    
                    indicator = strategy.getElement("EMSX_STRATEGY_FIELD_INDICATORS")
                    data = strategy.getElement("EMSX_STRATEGY_FIELDS")
                    
                    # Strategy parameters must be appended in the correct order. See the output 
                    # of GetBrokerStrategyInfo request for the order. The indicator value is 0 for 
                    # a field that carries a value, and 1 where the field should be ignored
                    
                    data.appendElement().setElement("EMSX_FIELD_DATA", "09:30:00")  # StartTime
                    indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 0)
                    data.appendElement().setElement("EMSX_FIELD_DATA", "10:30:00")   # EndTime
                    indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 0)
                    data.appendElement().setElement("EMSX_FIELD_DATA", "")           # Max%Volume
                    indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)
                    
                    data.appendElement().setElement("EMSX_FIELD_DATA", "")           # %AMSession
                    indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)
                    data.appendElement().setElement("EMSX_FIELD_DATA", "")           # OPG
                    indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)
                    data.appendElement().setElement("EMSX_FIELD_DATA", "")           # MOC
                    indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)
                    data.appendElement().setElement("EMSX_FIELD_DATA", "")           # CompletePX
                    indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)
                    
                    data.appendElement().setElement("EMSX_FIELD_DATA", "")           # TriggerPX
                    indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)
                    data.appendElement().setElement("EMSX_FIELD_DATA", "")           # DarkComplete
                    indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)
                    data.appendElement().setElement("EMSX_FIELD_DATA", "")           # DarkCompPX
                    indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)
                    data.appendElement().setElement("EMSX_FIELD_DATA", "")           # RefIndex
                    indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)
                    data.appendElement().setElement("EMSX_FIELD_DATA", "")           # Discretion
                    indicator.appendElement().setElement("EMSX_FIELD_INDICATOR", 1)
                        
                    print(f'Request: {request.toString()}')

                    self._message_route['request'] = request.toString()

                    self.request_id = blpapi.CorrelationId()

                    session.sendRequest(request, correlationId=self.request_id)
                else:
                    pass

            elif mesg.messageType() == SERVICE_OPEN_FAILURE:
                print >> sys.stderr, ('Error: Service Create Order failed to open')


    def _process_response_event(self, event):
        print('Processing RESPONSE event')

        for msg in event:

            if msg.correlationIds()[0].value() == self.request_id.value():

                if msg.messageType() == ERROR_INFO:
                    error_code = msg.getElementAsInteger('ERROR_CODE')
                    error_message = msg.getElementAsString('ERROR_MESSAGE')

                    print(f'Error code: {error_code}\nError message: {error_message}')

                if msg.messageType() == ROUTE_ORDER:
                    emsx_sequence = msg.getElementAsInteger('EMSX_SEQUENCE')
                    message = msg.getElementAsString('MESSAGE')
                    
                    print(f'Order Sequence: {emsx_sequence}\nMessage: {message}')
                

                global b_end
                b_end = True


    def _process_misc_event(self, event):
        print('Processing MISC event')

        for msg in event:
            print(msg.toString())


    def _send_response_message(self, message):
        print('Processing SEND_RESPONSE event')

        return 'SEND'

    def run_event(self):
        print('Processing RUN event')
        
        session_options = blpapi.SessionOptions()
        session_options.setServerHost(d_host)
        session_options.setServerPort(d_port)
        
        session = blpapi.Session(session_options, self._process_event)

        if not session.startAsync():
            return

        global b_end
        while b_end == False:
            pass

        session.stop()
        
        return self._message_route


def main():
    data = {
        'magic': 123456778,
        'ticker': 'XBV1 Index',
        'vol': 10,
        'order_type': 'MKT',
        'tif': 'DAY',
        'side': 'BUY',
        'broker': 'BMTB'
    }

    order_handler = CreateOrderHandler(data)
    data_msg = order_handler.run_event()

    route_handler = RouteOrderHandler(data_msg)
    route_handler.run_event()


main()
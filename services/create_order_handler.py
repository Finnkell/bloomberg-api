from __future__ import print_function
from __future__ import absolute_import

import os
import platform as plat
import sys
import blpapi

d_service = '//blp/emapisvc_beta'
d_host = 'localhost'
d_port = 8194
b_end = False


SESSION_STARTED         = blpapi.Name("SessionStarted")
SESSION_STARTUP_FAILURE = blpapi.Name("SessionStartupFailure")
SERVICE_OPENED          = blpapi.Name("ServiceOpened")
SERVICE_OPEN_FAILURE    = blpapi.Name("ServiceOpenFailure")
ERROR_INFO              = blpapi.Name("ErrorInfo")

CREATE_ORDER            = blpapi.Name("CreateOrder")


class CreateOrderHandler():

    def __init__(self, data):
        self.request_id = 0
        self.data       = data
        self._message   = {}

        self._message['_id'] = self.data['magic']


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
                request = service.createRequest("CreateOrder")

                request.set('EMSX_ORDER_REF_ID', self.data['magic'])
                request.set('EMSX_TICKER', self.data['ticker'])
                request.set('EMSX_AMOUNT', self.data['vol'])
                request.set('EMSX_ORDER_TYPE', self.data['order_type'])
                request.set('EMSX_TIF', self.data['tif'])
                request.set('EMSX_HAND_INSTRUCTION', 'ANY')
                request.set('EMSX_SIDE', self.data['side'])
                request.set('EMSX_BROKER', self.data['broker'])
                request.set('EMSX_NOTES', f'Envio de ordens do cliente {self.data["magic"]}')
                
                print(f'Request: {request.toString()}')

                self._message['request'] = {
                    'CreateOrder': {
                        'EMSX_ORDER_REF_ID': self.data['magic'],
                        'EMSX_TICKER': self.data['ticker'],
                        'EMSX_AMOUNT': self.data['vol'],
                        'EMSX_ORDER_TYPE': self.data['order_type'],
                        'EMSX_TIF': self.data['tif'],
                        'EMSX_HAND_INSTRUCTION': 'ANY',
                        'EMSX_SIDE': self.data['side'],
                        'EMSX_BROKER': self.data['broker'],
                        'EMSX_NOTES': f'Envio de ordens do cliente {self.data["magic"]}',
                    }
                }
                self.request_id = blpapi.CorrelationId()

                session.sendRequest(request, correlationId=self.request_id)

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

                if msg.messageType() == CREATE_ORDER:
                    emsx_sequence = msg.getElementAsInteger('EMSX_SEQUENCE')
                    message = msg.getElementAsString('MESSAGE')
                    
                    self._message['response'] = {
                        'CreateOrder': {
                            'EMSX_SEQUENCE': emsx_sequence,
                            'MESSAGE': message,
                        }
                    }

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

        return self._message


def main():
    data = {
        'magic': 123456778,
        'ticker': 'XBV1 Index',
        'vol': 1,
        'order_type': 'LMT',
        'tif': 'DAY',
        'side': 'BUY',
    }

    handler = CreateOrderHandler(data)

    handler.run_event()

# main()
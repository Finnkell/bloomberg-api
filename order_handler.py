from __future__ import print_function
from __future__ import absolute_import

from optparse import OptionParser

import os
import platform as plat
import sys
import blpapi
import sys

SERVICE_OPENED = blpapi.Name("ServiceOpened")

SECURITY_DATA  = blpapi.Name("securityData")
SECURITY       = blpapi.Name("security")
FIELD_DATA     = blpapi.Name("fieldData")
FIELD_ID       = blpapi.Name("fieldId")
OPT_CHAIN      = blpapi.Name("OPT_CHAIN")
SECURITY_DES   = blpapi.Name("Security Description")


def parseCmdLine():
    parser = OptionParser(description="Retrieve realtime data.")
    parser.add_option("-a",
                      "--ip",
                      dest="host",
                      help="server name or IP (default: %default)",
                      metavar="ipAddress",
                      default="localhost")
    parser.add_option("-p",
                      dest="port",
                      type="int",
                      help="server port (default: %default)",
                      metavar="tcpPort",
                      default=8194)
    parser.add_option("--me",
                      dest="maxEvents",
                      type="int",
                      help="stop after this many events (default: %default)",
                      metavar="maxEvents",
                      default=1000000)

    options,_ = parser.parse_args()

    return options


def main():
    options = parseCmdLine()

    # Fill SessionOptions
    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(options.host)
    sessionOptions.setServerPort(options.port)

    print("Connecting to %s:%d" % (options.host, options.port))

    # Create a Session
    session = blpapi.Session(sessionOptions)

    # Start a Session
    if not session.start():
        print("Failed to start session.")
        return

    if not session.openService('//blp/mktdata'):
        print('Couldn\'t Open mkt data session')
        return

    subscriptions = blpapi.SubscriptionList()
    subscriptions.add("VALE3 BZ Equity", "LAST_PRICE, OPEN, HIGH, LOW", "", blpapi.CorrelationId("VALE3 BZ Equity"))

    session.subscribe(subscriptions)

    try:
        # Process received events
        eventCount = 0
        while(True):
            # We provide timeout to give the chance to Ctrl+C handling:
            event = session.nextEvent(500)
            print("------ EVENT -------")

            for msg in event:
                if event.eventType() == blpapi.Event.SUBSCRIPTION_DATA:
                    print(msg.asElement().getElement("BID"))
                        
                    # print("%s - %s" % (msg.correlationIds()[0].value(), msg))
                else:
                    print(msg.asElement().elements())
            if event.eventType() == blpapi.Event.SUBSCRIPTION_DATA:
                eventCount += 1
                if eventCount >= options.maxEvents:
                    break
    finally:
        # Stop the session
        print("Session Stopped")
        session.stop()

if __name__ == "__main__":
    print("Subscribe")

    try:
        main()
    except KeyboardInterrupt:
        print("Ctrl+C pressed. Stopping...")

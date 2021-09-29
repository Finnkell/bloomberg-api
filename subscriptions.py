import blpapi
import sys


ORDER_ROUTE_FIELDS              = blpapi.Name("OrderRouteFields")

SLOW_CONSUMER_WARNING           = blpapi.Name("SlowConsumerWarning")
SLOW_CONSUMER_WARNING_CLEARED   = blpapi.Name("SlowConsumerWarningCleared")

SESSION_STARTED                 = blpapi.Name("SessionStarted")
SESSION_TERMINATED              = blpapi.Name("SessionTerminated")
SESSION_STARTUP_FAILURE         = blpapi.Name("SessionStartupFailure")
SESSION_CONNECTION_UP           = blpapi.Name("SessionConnectionUp")
SESSION_CONNECTION_DOWN         = blpapi.Name("SessionConnectionDown")

SERVICE_OPENED                  = blpapi.Name("ServiceOpened")
SERVICE_OPEN_FAILURE            = blpapi.Name("ServiceOpenFailure")

SUBSCRIPTION_FAILURE            = blpapi.Name("SubscriptionFailure")
SUBSCRIPTION_STARTED            = blpapi.Name("SubscriptionStarted")
SUBSCRIPTION_TERMINATED         = blpapi.Name("SubscriptionTerminated")

EXCEPTIONS = blpapi.Name("exceptions")
FIELD_ID = blpapi.Name("fieldId")
REASON = blpapi.Name("reason")
CATEGORY = blpapi.Name("category")
DESCRIPTION = blpapi.Name("description")

#d_service="//blp/emapisvc"
d_service="//blp/emapisvc_beta"
d_host="localhost"
d_port=8194
orderSubscriptionID=blpapi.CorrelationId(98)
routeSubscriptionID=blpapi.CorrelationId(99)


class SessionEventHandler(object):

    def processEvent(self, event, session):
        try:
            if event.eventType() == blpapi.Event.ADMIN:
                self.processAdminEvent(event)  
                
            elif event.eventType() == blpapi.Event.SESSION_STATUS:
                self.processSessionStatusEvent(event,session)
            
            elif event.eventType() == blpapi.Event.SERVICE_STATUS:
                self.processServiceStatusEvent(event,session)
                
            elif event.eventType() == blpapi.Event.SUBSCRIPTION_STATUS:
                self.processSubscriptionStatusEvent(event, session)

            elif event.eventType() == blpapi.Event.SUBSCRIPTION_DATA:
                self.processSubscriptionDataEvent(event)
            
            else:
                self.processMiscEvents(event)
                
        except:
            print ("Exception:  %s" % sys.exc_info()[0])
            
        return False



    def processAdminEvent(self,event):
        print ("Processing ADMIN event")

        for msg in event:
            
            if msg.messageType() == SLOW_CONSUMER_WARNING:
                print ("Warning: Entered Slow Consumer status")
            elif msg.messageType() ==  SLOW_CONSUMER_WARNING_CLEARED:
                print ("Slow consumer status cleared")
                
 
    def processSessionStatusEvent(self,event,session):
        print ("Processing SESSION_STATUS event")

        for msg in event:
            
            if msg.messageType() == SESSION_STARTED:
                print ("Session started...")
                session.openServiceAsync(d_service)
                
            elif msg.messageType() == SESSION_STARTUP_FAILURE:
                print ("Error: Session startup failed", file=sys.stderr)
                
            elif msg.messageType() == SESSION_TERMINATED:
                print ("Error: Session has been terminated")
                
            elif msg.messageType() == SESSION_CONNECTION_UP:
                print ("Session connection is up")
                
            elif msg.messageType() == SESSION_CONNECTION_DOWN:
                print ("Error: Session connection is down")
                
                

    def processServiceStatusEvent(self,event,session):
        print ("Processing SERVICE_STATUS event")
        
        for msg in event:
            
            if msg.messageType() == SERVICE_OPENED:
                print ("Service opened...")
                self.createOrderSubscription(session)
                
            elif msg.messageType() == SERVICE_OPEN_FAILURE:
                print ("Error: Service failed to open", file=sys.stderr)        
                
                
    def processSubscriptionStatusEvent(self, event, session):
        print ("Processing SUBSCRIPTION_STATUS event")

        for msg in event:
            
            if msg.messageType() == SUBSCRIPTION_STARTED:
                
                if msg.correlationIds()[0].value() == orderSubscriptionID.value():
                    print ("Order subscription started successfully")
                    self.createRouteSubscription(session)
                    
                elif msg.correlationIds()[0].value() == routeSubscriptionID.value():
                    print ("Route subscription started successfully")
                    
            elif msg.messageType() == SUBSCRIPTION_FAILURE:
                print ("Error: Subscription failed", file=sys.stderr)
                print ("MESSAGE: %s" % (msg), file=sys.stderr)
                    
                reason = msg.getElement("reason");
                errorcode = reason.getElementAsInteger("errorCode")
                description = reason.getElementAsString("description")
            
                print ("Error: (%d) %s" % (errorcode, description), file=sys.stderr)                
                
            elif msg.messageType() == SUBSCRIPTION_TERMINATED:
                print ("Error: Subscription terminated", file=sys.stderr)
                print ("MESSAGE: %s" % (msg), file=sys.stderr)


    def processSubscriptionDataEvent(self, event):
        #print ("Processing SUBSCRIPTION_DATA event")
        
        for msg in event:
            
            if msg.messageType() == ORDER_ROUTE_FIELDS:
                
                event_status = msg.getElementAsInteger("EVENT_STATUS")

                print(f"Event Status: {event_status}")
                
                if event_status == 1:
                    print ("====================")
                    
                    if msg.correlationIds()[0].value() == orderSubscriptionID.value():

                        emsx_status = msg.getElementAsString("EMSX_STATUS") if msg.hasElement("EMSX_STATUS") else ""
                        
                        
                        print ("ORDER MESSAGE: CorrelationID(%d)   Status(%d)" % (msg.correlationIds()[0].value(),event_status))
                        print ("MESSAGE: %s" % (msg))

                        print ("EMSX_STATUS: %s" % (emsx_status))
            
                    elif msg.correlationIds()[0].value() == routeSubscriptionID.value():

                        emsx_status = msg.getElementAsString("EMSX_STATUS") if msg.hasElement("EMSX_STATUS") else ""
                        
                        print ("ROUTE MESSAGE: CorrelationID(%d)   Status(%d)" % (msg.correlationIds()[0].value(),event_status))
                        print ("MESSAGE: %s" % (msg))
                        
                        print ("EMSX_STATUS: %s" % (emsx_status))

                
                    if msg.correlationIds()[0].value() == orderSubscriptionID.value():
                        #print ("O", end=".",)
                        print ("O."),
                        pass
                    elif msg.correlationIds()[0].value() == routeSubscriptionID.value():
                        #print ("R", end=".",)
                        print ("R."),
                        pass
                    
                elif event_status == 11:
                
                    if msg.correlationIds()[0].value() == orderSubscriptionID.value():
                        print ("Order - End of initial paint")
                    elif msg.correlationIds()[0].value() == routeSubscriptionID.value():
                        print ("Route - End of initial paint")

                else:
                    print ("")
                    
                    if msg.correlationIds()[0].value() == orderSubscriptionID.value():

                        emsx_status = msg.getElementAsString("EMSX_STATUS") if msg.hasElement("EMSX_STATUS") else ""
                        
                        
                        print ("ORDER MESSAGE: CorrelationID(%d)   Status(%d)" % (msg.correlationIds()[0].value(),event_status))
                        print ("MESSAGE: %s" % (msg))

                        print ("EMSX_STATUS: %s" % (emsx_status))
            
                    elif msg.correlationIds()[0].value() == routeSubscriptionID.value():

                        emsx_status = msg.getElementAsString("EMSX_STATUS") if msg.hasElement("EMSX_STATUS") else ""
                        
                        print ("ROUTE MESSAGE: CorrelationID(%d)   Status(%d)" % (msg.correlationIds()[0].value(),event_status))
                        print ("MESSAGE: %s" % (msg))
                        
                        print ("EMSX_STATUS: %s" % (emsx_status))

            else:
                print ("Error: Unexpected message", file=sys.stderr)


    def processMiscEvents(self, event):
        
        print ("Processing " + event.eventType() + " event")
        
        for msg in event:

            print ("MESSAGE: %s" % (msg))


    def createOrderSubscription(self, session):
        
        print ("Create Order subscription")
        
        #orderTopic = d_service + "/order;team=TKTEAM?fields="
        orderTopic = d_service + "/order?fields="
        orderTopic = orderTopic + "EMSX_STATUS,"

        subscriptions = blpapi.SubscriptionList()
        
        subscriptions.add(topic=orderTopic,correlationId=orderSubscriptionID)

        session.subscribe(subscriptions)


    def createRouteSubscription(self, session):
        
        print ("Create Route subscription")
        
        #routeTopic = d_service + "/route;team=EMSX_API?fields="
        routeTopic = d_service + "/route?fields="
        routeTopic = routeTopic + "EMSX_STATUS,"

        subscriptions = blpapi.SubscriptionList()
        
        subscriptions.add(topic=routeTopic,correlationId=routeSubscriptionID)

        session.subscribe(subscriptions)

def main():

    sessionOptions = blpapi.SessionOptions()
    sessionOptions.setServerHost(d_host)
    sessionOptions.setServerPort(d_port)

    print ("Connecting to %s:%d" % (d_host,d_port))

    eventHandler = SessionEventHandler()

    session = blpapi.Session(sessionOptions, eventHandler.processEvent)
    
    if not session.startAsync():
        print ("Failed to start session.")
        return

    try:
        # Wait for enter key to exit application
        print ("Press ENTER to quit")
        input()
    finally:
        session.stop()

if __name__ == "__main__":
    print ("Bloomberg - EMSX API Example - EMSXSubscriptions")

    try:
        main()
    except KeyboardInterrupt:
        print ("Ctrl+C pressed. Stopping...")
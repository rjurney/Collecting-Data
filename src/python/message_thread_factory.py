class MessageThreadFactory(Borg):
    # Default settings
    _thres = 0.25
    _time_delta = datetime.timedelta(days=2)
    def __init__(self,thres=None,time_delta=None,verbose=False):
        self._db = DB.DB()
        if thres != None:
            self._thres = thres
        if time_delta != None:
            self._time_delta = time_delta
        self._verbose = verbose
        
    def setThres(self,thres):
        """Sets the maximum distance that leads to the assignment of two messages 
        to the same thread."""
        self._thres = thres
        
    def setTimeDelta(self,time_delta):
        """Sets the maximum time differential considered between two candidate 
        messages."""
        self._time_delta = time_delta
        
    def constructConversationThreads(self,msgs):
        """Constructs conversation threads in the message collection msgs by pairing 
        messages where metric(subject1,subject2) < thres, the message datetime 
        differential is less than time_delta and the sender of each message in the 
        pair is listed in the To field of the other message."""
        
        # Time order the message ids
        msgs.timeOrder()
        
        # Create another copy of the message collection for iteration
        msgs2 = msgs.copy()
        
        # Create thread graph
        tgraph = nx.Graph()
        
        # Begin pairwise comparisons
        i = 0
        num_msgs = msgs.getNumberOfMsgs()
        while i < num_msgs-1:
            
            # Select the earlier message
            msg1 = msgs[i]
            
            # If there's no datetime for this message, continue
            if msg1.Datetime == None:
                i += 1
                continue
            
            # Scan ahead looking for potential matches
            j = i+1
            while j < num_msgs:
                # If there's no datetime for this message, continue
                msg2 = msgs2[j]
                if msg2.Datetime == None:
                    j += 1
                    continue
                if self._verbose:
                    print "Message Pair (%d,%d):" % (i,j)
                # If the message time differental exceeds time_delta, advance msg1
                # and start scanning again.
                td = msg2.Datetime - msg1.Datetime
                if self._verbose:
                    print "Time Differential: %s" % str(td)
                if td > self._time_delta:
                    if self._verbose:
                        print "Exceeds time delta threshold!"
                        print
                    j = num_msgs
                    continue
                # If the sender of one of the messages in the pair is not a recipient of 
                # the other or the message senders match, move on to check the next pair.
                msg1_recips = msg1.TO
                msg2_recips = msg2.TO
                if ((not msg1.Sender in msg2_recips) or (not msg2.Sender in msg1_recips) or
                    (msg1.Sender == msg2.Sender)):
                    if self._verbose:
                        print "No bidirectional sender-recipient match!"
                        print
                    j += 1
                    continue
                
                # Lookup the longest common substring between the message subject lines. 
                # Compute the LCS if it is not available.
                pair = [msg1.MessageID,msg2.MessageID]
                lcs = self._db.getMessagePairLCSubstring(pair,'Subject')[0]
                if lcs == None:
                    print "Warning: longest common substring not available in the database."
                dist = lcs_dist(msg1.Subject,msg2.Subject,lcs)
                if self._verbose:
                    print "Distance: %f" % dist
                    print "Message 1 Subject: %s" % msg1.Subject
                    print "Message 2 Subject: %s" % msg2.Subject
                    print "Longest Common Substring: %s" % lcs
                
                # Check to see if the distance is above or below threshold
                if dist > self._thres:
                    if self._verbose:
                        print "String distance above threshold!"
                        print
                    j += 1
                    continue
                    
                # Add message nodes and an edge between them to the thread graph if we've 
                # made it this far
                if self._verbose:
                    print "Match!"
                    print
                tgraph.add_node(msg1.MessageID)
                tgraph.add_node(msg2.MessageID)
                tgraph.add_edge(msg2.MessageID,msg1.MessageID)
                
                j += 1
                
            i += 1
                
        # Identify the individual threads by computing the connected components of tgraph
        threads = nx.algorithms.components.connected.connected_component_subgraphs(tgraph)                
                
        # Create the corresponding thread objects
        threads = [Thread(t) for t in threads]
                
        return threads

# protocol-specific...
import protocols.iftcache

sender = protocols.iftcache.iftcache_sender()
receiver = protocols.iftcache.iftcache_receiver()

# invariant...
print "sender requires: " + str(sender.get_setup_attrs())
print "receiver requires: " + str(receiver.get_setup_attrs())

# note: {all attrs} = {setup attrs} + {connect attrs} + {supported attrs}; solve for {supported attrs}
print "sender supports: " + str(set(sender.get_all_attrs()) - set(sender.get_send_attrs() + sender.get_connect_attrs()))
print "receiver supports: " + str(set(receiver.get_all_attrs()) - set(receiver.get_recv_attrs() + receiver.get_connect_attrs()))

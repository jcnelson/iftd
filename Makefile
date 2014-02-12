IFTD_PKGS_DIR=rpm/RPMS/noarch

RAVENBUILD_IFTD=raven/iftd/build/iftd
RAVENBUILD_IFTD_PKGS=raven/iftd/packages

RAVENBUILD_IFTD_NEST=raven/iftd-nest/build/iftd-nest
RAVENBUILD_IFTD_NEST_PKGS=raven/iftd-nest/packages

RAVENBUILD_AZ_IFTD=raven/arizonatransfer_iftd/build/arizonatransfer_iftd
RAVENBUILD_AZ_IFTD_PKGS=raven/arizonatransfer_iftd/packages

RAVENBUILD_AZ_NEST=raven/arizonatransfer_nest/build/arizonatransfer_nest
RAVENBUILD_AZ_NEST_PKGS=raven/arizonatransfer_nest/packages

all: arizonatransfer-iftd-rpm arizonatransfer-nest-rpm iftd-common-rpm iftd-rpm iftd-owl-rpm
all-ravenbuild: iftd-ravenbuild iftd-nest-ravenbuild arizonatransfer-iftd-ravenbuild arizonatransfer-nest-ravenbuild

clean-ravenbuild:
	/bin/rm -f $(RAVENBUILD_IFTD_PKGS)/*.rpm
	/bin/rm -rf $(RAVENBUILD_IFTD)/etc
	/bin/rm -rf $(RAVENBUILD_IFTD)/usr

clean:
	/bin/rm -f $(IFTD_PKGS_DIR)/*.rpm 

iftd-ravenbuild:
	rm -rf $(RAVENBUILD_IFTD)/usr
	rm -rf $(RAVENBUILD_IFTD)/etc
	rm -rf $(RAVENBUILD_IFTD_PKGS)/*.rpm
	mkdir -p $(RAVENBUILD_IFTD)/usr/bin
	mkdir -p $(RAVENBUILD_IFTD)/usr/sbin
	mkdir -p $(RAVENBUILD_IFTD)/etc/iftd
	mkdir -p $(RAVENBUILD_IFTD)/etc/init.d/
	mkdir -p $(RAVENBUILD_IFTD)/etc/cron.daily/
	mkdir -p $(RAVENBUILD_IFTD)/usr/lib/python2.5/site-packages/iftd
	mkdir -p $(RAVENBUILD_IFTD)/usr/lib/python2.5/site-packages/iftd/protocols
	mkdir -p $(RAVENBUILD_IFTD)/usr/lib/python2.5/site-packages/iftd/classifiers
	mkdir -p $(RAVENBUILD_IFTD)/usr/lib/python2.5/site-packages/iftd/iftcore
	cp -a iftd $(RAVENBUILD_IFTD)/usr/sbin
	cp -a iftool $(RAVENBUILD_IFTD)/usr/bin
	cp -a cache/squid.fc8.conf $(RAVENBUILD_IFTD)/etc/iftd
	cp -a iftd_client.xml $(RAVENBUILD_IFTD)/etc/iftd/iftd.xml
	cp -a util/etc/init.d/iftd $(RAVENBUILD_IFTD)/etc/init.d/
	cp -a util/etc/cron.daily/iftd $(RAVENBUILD_IFTD)/etc/cron.daily
	cp -a *.py $(RAVENBUILD_IFTD)/usr/lib/python2.5/site-packages/iftd/
	cp -a protocols/*.py $(RAVENBUILD_IFTD)/usr/lib/python2.5/site-packages/iftd/protocols
	cp -a classifiers/*.py classifiers/LICENSE.txt $(RAVENBUILD_IFTD)/usr/lib/python2.5/site-packages/iftd/classifiers
	cp -a iftcore/*.py $(RAVENBUILD_IFTD)/usr/lib/python2.5/site-packages/iftd/iftcore

iftd-nest-ravenbuild:
	rm -rf $(RAVENBUILD_IFTD_NEST)/usr
	rm -rf $(RAVENBUILD_IFTD_NEST)/etc
	rm -rf $(RAVENBUILD_IFTD_NEST_PKGS)/*.rpm
	mkdir -p $(RAVENBUILD_IFTD_NEST)/usr/bin
	mkdir -p $(RAVENBUILD_IFTD_NEST)/usr/sbin
	mkdir -p $(RAVENBUILD_IFTD_NEST)/etc/iftd
	mkdir -p $(RAVENBUILD_IFTD_NEST)/etc/init.d/
	mkdir -p $(RAVENBUILD_IFTD_NEST)/etc/cron.daily/
	mkdir -p $(RAVENBUILD_IFTD_NEST)/usr/lib/python2.5/site-packages/iftd
	mkdir -p $(RAVENBUILD_IFTD_NEST)/usr/lib/python2.5/site-packages/iftd/protocols
	mkdir -p $(RAVENBUILD_IFTD_NEST)/usr/lib/python2.5/site-packages/iftd/classifiers
	mkdir -p $(RAVENBUILD_IFTD_NEST)/usr/lib/python2.5/site-packages/iftd/iftcore
	cp -a iftd $(RAVENBUILD_IFTD_NEST)/usr/sbin
	cp -a iftool $(RAVENBUILD_IFTD_NEST)/usr/bin
	cp -a cache/squid.fc8.conf $(RAVENBUILD_IFTD_NEST)/etc/iftd
	cp -a iftd_nest.xml $(RAVENBUILD_IFTD_NEST)/etc/iftd/iftd.xml
	cp -a util/etc/init.d/iftd $(RAVENBUILD_IFTD_NEST)/etc/init.d/
	cp -a util/etc/cron.daily/iftd $(RAVENBUILD_IFTD_NEST)/etc/cron.daily
	cp -a *.py $(RAVENBUILD_IFTD_NEST)/usr/lib/python2.5/site-packages/iftd/
	cp -a protocols/*.py $(RAVENBUILD_IFTD_NEST)/usr/lib/python2.5/site-packages/iftd/protocols
	cp -a classifiers/*.py classifiers/LICENSE.txt $(RAVENBUILD_IFTD_NEST)/usr/lib/python2.5/site-packages/iftd/classifiers
	cp -a iftcore/*.py $(RAVENBUILD_IFTD_NEST)/usr/lib/python2.5/site-packages/iftd/iftcore

arizonatransfer-iftd-ravenbuild:
	rm -rf $(RAVENBUILD_AZ_IFTD)/usr
	mkdir -p $(RAVENBUILD_AZ_IFTD)/usr/lib/python2.5/site-packages/transfer
	cp -a arizonatransfer/arizonatransfer_iftd.py $(RAVENBUILD_AZ_IFTD)/usr/lib/python2.5/site-packages/transfer


arizonatransfer-nest-ravenbuild:
	rm -rf $(RAVENBUILD_AZ_NEST)/usr
	mkdir -p $(RAVENBUILD_AZ_NEST)/usr/lib/python2.5/site-packages/transfer
	cp -a arizonatransfer/arizonatransfer_nest.py $(RAVENBUILD_AZ_NEST)/usr/lib/python2.5/site-packages/transfer

remove-iftd: remove-iftd-setup
	rpmbuild -bb rpm/SPECS/remove-iftd-*

remove-iftd-setup:
	if [ -d /tmp/remove-iftd ]; then rm -rf /tmp/remove-iftd; fi
	if [ -f remove-iftd.tar.gz ]; then rm -rf remove-iftd.tar.gz; fi
	if ! [ -f ~/.rpmmacros ]; then echo "%_topdir $(PWD)/rpm" > ~/.rpmmacros; fi
	mkdir -p /tmp/remove-iftd/tmp/
	touch /tmp/remove-iftd/tmp/iftd-remove-log.log
	tar cf remove-iftd.tar -C /tmp/remove-iftd tmp
	gzip remove-iftd.tar
	mv remove-iftd.tar.gz rpm/SOURCES

iftd-owl-rpm: iftd-owl-rpm-setup
	rpmbuild -bb rpm/SPECS/iftd-owl.spec

iftd-owl-rpm-setup:
	if [ -d /tmp/iftd-owl ]; then rm -rf /tmp/iftd-owl; fi
	if [ -f iftd-owl.tar.gz ]; then rm -rf iftd-owl.tar.gz; fi
	if ! [ -f ~/.rpmmacros ]; then echo "%_topdir $(PWD)/rpm" > ~/.rpmmacros; fi
	mkdir -p /tmp/iftd-owl/usr/local/owl/scripts.d/
	cp -a util/usr/local/owl/scripts.d/iftd /tmp/iftd-owl/usr/local/owl/scripts.d/
	tar cf iftd-owl.tar -C /tmp/iftd-owl usr 
	gzip iftd-owl.tar
	mv iftd-owl.tar.gz rpm/SOURCES

iftd-rpm: iftd-rpm-setup
	rpmbuild -bb rpm/SPECS/iftd.spec

iftd-rpm-setup:
	if [ -d /tmp/iftd ]; then rm -rf /tmp/iftd; fi
	if [ -f iftd.tar.gz ]; then rm -rf iftd.tar.gz; fi
	if ! [ -f ~/.rpmmacros ]; then echo "%_topdir $(PWD)/rpm" > ~/.rpmmacros; fi
	mkdir -p /tmp/iftd/usr/sbin
	mkdir -p /tmp/iftd/usr/bin
	mkdir -p /tmp/iftd/etc/iftd
	mkdir -p /tmp/iftd/etc/init.d/
	mkdir -p /tmp/iftd/etc/cron.daily/
	cp -a iftd /tmp/iftd/usr/sbin
	cp -a iftool /tmp/iftd/usr/bin
	cp -a iftd.xml /tmp/iftd/etc/iftd
	cp -a cache/squid.fc8.conf /tmp/iftd/etc/iftd/squid.conf
	cp -a util/etc/init.d/iftd /tmp/iftd/etc/init.d/
	cp -a util/etc/cron.daily/iftd /tmp/iftd/etc/cron.daily/
	tar cf iftd.tar -C /tmp/iftd etc usr 
	gzip iftd.tar
	mv iftd.tar.gz rpm/SOURCES

arizonatransfer-nest-rpm: arizonatransfer-nest-setuprpm
	rpmbuild -bb rpm/SPECS/arizonatransfer-nest.spec

arizonatransfer-nest-setuprpm:
	if [ -d /tmp/arizonatransfer-nest ]; then rm -rf /tmp/arizonatransfer-nest; fi
	if [ -f arizonatransfer-nest.tar.gz ]; then rm -rf arizonatransfer-nest.tar.gz; fi
	if ! [ -f ~/.rpmmacros ]; then echo "%_topdir $(PWD)/rpm" > ~/.rpmmacros; fi
	mkdir -p /tmp/arizonatransfer-nest/transfer
	cp -a arizonatransfer/arizonatransfer_nest.py /tmp/arizonatransfer-nest/transfer/
	tar cf arizonatransfer-nest.tar -C /tmp/arizonatransfer-nest transfer
	gzip arizonatransfer-nest.tar
	mv arizonatransfer-nest.tar.gz rpm/SOURCES

arizonatransfer-iftd-rpm: arizonatransfer-iftd-setuprpm
	rpmbuild -bb rpm/SPECS/arizonatransfer-iftd.spec

arizonatransfer-iftd-setuprpm:
	if [ -d /tmp/arizonatransfer-iftd ]; then rm -rf /tmp/arizonatransfer-iftd; fi
	if [ -f arizonatransfer-iftd.tar.gz ]; then rm -rf arizonatransfer-iftd.tar.gz; fi
	if ! [ -f ~/.rpmmacros ]; then echo "%_topdir $(PWD)/rpm" > ~/.rpmmacros; fi
	mkdir -p /tmp/arizonatransfer-iftd/transfer
	cp -a arizonatransfer/arizonatransfer_iftd.py /tmp/arizonatransfer-iftd/transfer/
	tar cf arizonatransfer-iftd.tar -C /tmp/arizonatransfer-iftd transfer
	gzip arizonatransfer-iftd.tar
	mv arizonatransfer-iftd.tar.gz rpm/SOURCES

iftd-common-rpm: iftd-common-setuprpm
	rpmbuild -bb rpm/SPECS/iftd-common.spec

iftd-common-setuprpm:
	if [ -d /tmp/iftd-common ]; then rm -rf /tmp/iftd-common; fi
	if [ -f iftd-common.tar.gz ]; then rm -rf iftd-common.tar.gz; fi
	if ! [ -f ~/.rpmmacros ]; then echo "%_topdir $(PWD)/rpm" > ~/.rpmmacros; fi
	mkdir -p /tmp/iftd-common/iftd
	mkdir /tmp/iftd-common/iftd/protocols  
	mkdir /tmp/iftd-common/iftd/classifiers 
	mkdir /tmp/iftd-common/iftd/iftcore 
	cp -a *.py /tmp/iftd-common/iftd
	cp -a protocols/*.py /tmp/iftd-common/iftd/protocols 
	cp -a classifiers/*.py /tmp/iftd-common/iftd/classifiers 
	cp -a iftcore/*.py /tmp/iftd-common/iftd/iftcore 
	tar cf iftd-common.tar -C /tmp/iftd-common iftd
	gzip iftd-common.tar
	mv iftd-common.tar.gz rpm/SOURCES
	rm -rf /tmp/iftd-common

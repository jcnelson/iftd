Summary: iftd common files
Name: iftd-common
Version: 0.99
Release: 1
License: GPL
Group: Development/Tools
Requires: python
BuildArch: noarch
Buildroot: /tmp/iftd-common
Source: iftd-common.tar.gz

%define instdir /usr/lib/python2.5/site-packages/

%description
Python packages and common protocols used by iftd.

%build
if [ -d $RPM_BUILD_ROOT ]; then rm -rf $RPM_BUILD_ROOT; fi
mkdir -p $RPM_BUILD_ROOT/%{instdir}
tar xvf $RPM_SOURCE_DIR/iftd-common.tar.gz -C $RPM_BUILD_ROOT/%{instdir}

%install
mkdir -p %{instdir}

%post
if [ -f /etc/init.d/iftd ]; then
   /etc/init.d/iftd restart
fi

%files
%defattr(-,root,root)
%{instdir}/iftd/protocols/bittorrent.py
%{instdir}/iftd/protocols/http.py
%{instdir}/iftd/protocols/__init__.py
%{instdir}/iftd/protocols/iftsocket.py
%{instdir}/iftd/protocols/iftscp.py
%{instdir}/iftd/protocols/iftcache.py
%{instdir}/iftd/protocols/raven.py
%{instdir}/iftd/protocols/iftgush.py
%{instdir}/iftd/protocols/nest_helper.py
%{instdir}/iftd/iftstats.py
%{instdir}/iftd/iftfile.py
%{instdir}/iftd/iftlog.py
%{instdir}/iftd/iftdata.py
%{instdir}/iftd/iftutil.py
%{instdir}/iftd/iftloader.py
%{instdir}/iftd/iftapi.py
%{instdir}/iftd/ifttransfer.py
%{instdir}/iftd/iftcore/__init__.py
%{instdir}/iftd/iftcore/consts.py
%{instdir}/iftd/iftcore/iftreceiver.py
%{instdir}/iftd/iftcore/iftsender.py
%{instdir}/iftd/iftcore/ifttransmit.py
%{instdir}/iftd/classifiers/api.py
%{instdir}/iftd/classifiers/compat.py
%{instdir}/iftd/classifiers/__init__.py
%{instdir}/iftd/classifiers/internals.py
%{instdir}/iftd/classifiers/naivebayes.py
%{instdir}/iftd/classifiers/decisiontree.py
%{instdir}/iftd/classifiers/probability.py
%{instdir}/iftd/classifiers/util.py
%{instdir}/iftd/__init__.py

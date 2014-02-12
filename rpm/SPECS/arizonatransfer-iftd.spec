Summary: IFTD plugin for Raven package transfer
Name: arizonatransfer-iftd
Version: 0.99
Release: 1
License: GPL
Group: Development/Tools
Requires: python iftd-common
BuildArch: noarch
Buildroot: /tmp/arizonatransfer-iftd/
Source: arizonatransfer-iftd.tar.gz

%define instdir /usr/lib/python2.5/site-packages/

%description
Transfer module for Raven that makes use of IFTD.

%build
if [ -d $RPM_BUILD_ROOT ]; then rm -rf $RPM_BUILD_ROOT; fi
mkdir -p $RPM_BUILD_ROOT/%{instdir}
tar xvf $RPM_SOURCE_DIR/arizonatransfer-iftd.tar.gz -C $RPM_BUILD_ROOT/%{instdir}

%install
mkdir -p %{instdir}

%files
%defattr(-,root,root)
%{instdir}/transfer/arizonatransfer_iftd.py

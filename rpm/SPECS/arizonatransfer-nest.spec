Summary: Nest proxy plugin for Stork client package transfer that invokes IFTD.
Name: arizonatransfer-nest
Version: 0.99
Release: 1
License: GPL
Group: Development/Tools
Requires: python iftd-common
BuildArch: noarch
Buildroot: /tmp/arizonatransfer-nestproxy/
Source: arizonatransfer-nest.tar.gz

%define instdir /usr/lib/python2.5/site-packages/

%description
Transfer module for Raven that calls out to IFTD on the nest.

%build
if [ -d $RPM_BUILD_ROOT ]; then rm -rf $RPM_BUILD_ROOT; fi
mkdir -p $RPM_BUILD_ROOT/%{instdir}
tar xvf $RPM_SOURCE_DIR/arizonatransfer-nest.tar.gz -C $RPM_BUILD_ROOT/%{instdir}

%install
mkdir -p %{instdir}

%files
%defattr(-,root,root)
%{instdir}/transfer/arizonatransfer_nest.py

Summary: intelligent file transfer daemon--monitoring scripts
Name: iftd-owl
Version: 0.01
Release: 4
License: GPL
Group: Development/Tools
Requires: iftd
BuildArch: noarch
Buildroot: /tmp/iftd-owl/
Source: iftd-owl.tar.gz

%define instdir /

%description
Scripts for owld to monitor the Intelligent File Transfer Daemon.

%build
if [ -d $RPM_BUILD_ROOT ]; then rm -rf $RPM_BUILD_ROOT; fi
mkdir -p $RPM_BUILD_ROOT/%{instdir}
tar xvf $RPM_SOURCE_DIR/iftd-owl.tar.gz -C $RPM_BUILD_ROOT/%{instdir}

%files
%defattr(-,root,root)
%{instdir}/usr/local/owl/scripts.d/iftd

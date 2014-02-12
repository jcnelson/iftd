Summary: intelligent file transfer daemon
Name: iftd
Version: 0.99
Release: 1
License: GPL
Group: Development/Tools
Requires: python, iftd-common, squid, python-lxml
BuildArch: noarch
Buildroot: /tmp/iftd/
Source: iftd.tar.gz

%define instdir /

%description
Intelligent File Transfer Daemon.  Server for protocol-agnostic file transmission.

%build
if [ -d $RPM_BUILD_ROOT ]; then rm -rf $RPM_BUILD_ROOT; fi
mkdir -p $RPM_BUILD_ROOT/%{instdir}
tar xvf $RPM_SOURCE_DIR/iftd.tar.gz -C $RPM_BUILD_ROOT/%{instdir}

%post
SQUID_CONF="/etc/squid/squid.conf"
IFTD_SQUIDCONF="/etc/iftd/squid.conf"
IFTD_CONF="/etc/iftd/iftd.xml"
SQUID_LOCKFILE="/var/lock/subsys/squid"

if [ -f $SQUID_CONF ]; then 
   mv $SQUID_CONF $SQUID_CONF.bak
fi

ln -s $IFTD_SQUIDCONF $SQUID_CONF

if ! [ -f $SQUID_LOCKFILE ]; then
   echo "Squid lockfile $SQUID_LOCKFILE not found."
   echo "Attempting to start Squid."
   /etc/init.d/squid start
else
   echo "Reconfiguring Squid to use IFTD's squid.conf"
   /usr/sbin/squid -k reconfigure $SQUID_CONF
fi

/etc/init.d/iftd stop

mkdir /tmp/iftd-files
mkdir /tmp/iftd-send
mkdir /tmp/iftd-recv
chmod 666 /tmp/iftd-files
chmod 666 /tmp/iftd-send
chmod 666 /tmp/iftd-recv

/etc/init.d/iftd start

%preun
/etc/init.d/iftd stop


%files
%defattr(-,root,root)
%{instdir}/usr/sbin/iftd
%{instdir}/usr/bin/iftool
%{instdir}/etc/iftd/iftd.xml
%{instdir}/etc/iftd/squid.conf
%{instdir}/etc/init.d/iftd
%{instdir}/etc/cron.daily/iftd

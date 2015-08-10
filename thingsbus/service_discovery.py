from __future__ import absolute_import
import dns.resolver


class ServiceFinder(object):
    RESOLVER = dns.resolver.Resolver()

    @classmethod
    def broker_url(cls, domain):
        return cls._get_broker_url(domain, 'tcp', 'thingsbus')

    @classmethod
    def broker_input_url(cls, domain, protocol='tcp'):
        return cls._get_broker_url(domain, protocol, 'thingsbus_input')

    @classmethod
    def _srv_query(cls, fqdn):
        a = cls.RESOLVER.query(fqdn, 'SRV')
        # TODO actually do the priority and weight SRV record thing
        use_rr = a.rrset[0]
        return (use_rr.target.to_text(), use_rr.port)

    @classmethod
    def _get_broker_url(cls, domain, proto, service):
        fqdn = '_%s._%s.%s' % (service, proto, domain)
        host, port = cls._srv_query(fqdn)

        return '%s://%s:%d' % (proto, host, port)

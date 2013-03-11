import ccrawler.common.settings as common_settings
#import ccrawler.utils.misc as misc

class DomainDecodingCache(object):
    _data_key = "decoding"

    @classmethod
    def get_domain_decoding(cls, domain):
        all_decodings = common_settings.cache_client().get(DomainDecodingCache._data_key, domain)
        if all_decodings is None or len(all_decodings) == 0:
            return None
        else:
           max_decoding = max(all_decodings.items(), key = lambda pair : int(pair[1]))
           return max_decoding[0]

    @classmethod
    def inc_domain_decoding(cls, domain, decoding):
        common_settings.cache_client().set(DomainDecodingCache._data_key, domain, inc_map = {decoding : 1})

    @classmethod
    def delete_domain(cls, domain):
        common_settings.cache_client().delete(DomainDecodingCache._data_key, domain)

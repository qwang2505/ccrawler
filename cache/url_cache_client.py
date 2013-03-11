import ccrawler.common.settings as common_settings
import ccrawler.utils.misc as misc
import copy

def _get_cache_fields():
    field_configs = common_settings.redis_data_config["data_types"]["url"]["fields"]
    return map(lambda field_config : field_config if isinstance(field_config, str) else field_config[0], field_configs)

class UrlCacheClient(object):
    _fields = _get_cache_fields()

    @classmethod
    def check_url_exists(cls, ori_url):
        url = copy.deepcopy(ori_url)
        success = common_settings.cache_client().set("url_dedup", url, data = "1", nx = True)
        if success:
            return False, True
        else:
            alive = common_settings.cache_client().get("url_dedup", url) == "1"
            return True, alive

    @classmethod
    def get_url_info(cls, ori_url, fields=None):
        """
        found: returns object
        not_in_cache: returns None
        """

        url = copy.deepcopy(ori_url)

        if fields == None:
            fields = UrlCacheClient._fields
        elif not misc.subset(fields, UrlCacheClient._fields):
            return None

        return common_settings.cache_client().get("url", url, fields=fields)

    @classmethod
    def get_url_info_by_status(cls, ori_url, crawl_status, fields):
        """
        found: returns object
        cond not met: returns False
        not_in_cache: returns None
        """

        cond = {"fields" : ["crawl_status"], "func" : lambda url_info : url_info["crawl_status"] == crawl_status}
        return UrlCacheClient._get_url_info_by_cond(ori_url, cond, fields)

    @classmethod
    def _get_url_info_by_cond(cls, url, cond, fields):
        if cond is not None:
            inserted_cond_fields = []
            for field_name in cond["fields"]:
                if not field_name in fields:
                    fields.append(field_name)
                    inserted_cond_fields.append(field_name)

        url_info = UrlCacheClient.get_url_info(url, fields)

        if url_info is not None:
            if cond is not None:
                if not cond["func"](url_info):
                    return False

                for field_name in inserted_cond_fields:
                    url_info.pop(field_name)
            return url_info
        else:
            return None

    @classmethod
    def _update_url_info(cls, url, update_map, inc_map, cond=None, with_get=False, fields=[]):
        deleting = update_map.has_key("crawl_status") and update_map["crawl_status"] in ["failed", "notAlive"]
        if not deleting:
            cache_update_map = misc.clone_dict(update_map, UrlCacheClient._fields, soft=True)
            cache_inc_map = misc.clone_dict(inc_map, UrlCacheClient._fields, soft=True)
        else:
            cache_update_map = {}
            cache_inc_map = {}

        ret_value = common_settings.cache_client().set("url", url, update_map = cache_update_map, inc_map = cache_inc_map, cond = cond, with_get = with_get, fields = fields)

        if deleting:
            common_settings.cache_client().delete("url", url)
            common_settings.cache_client().set("url_dedup", url, data = "0")

        return ret_value

    @classmethod
    def update_url_info(cls, url, update_map, inc_map = {}):
        """
        returns True/False if update succeeded
        """

        if inc_map is None:
            inc_map = {}

        return UrlCacheClient._update_url_info(url, update_map, inc_map)

    @classmethod
    def update_url_info_by_status(cls, url, crawl_status, update_map, inc_map = {}):
        """
        returns True/False if update succeeded
        returns False if cond not met
        """

        cond = {"fields" : ["crawl_status"], "func" : lambda url_info : url_info["crawl_status"] == crawl_status if url_info is not None else False}
        return UrlCacheClient._update_url_info(url, update_map, inc_map, cond)

    @classmethod
    def _find_and_modify_by_cond(cls, url, cond, update_map, inc_map, fields):
        if fields is None:
            fields = UrlCacheClient._fields

        if inc_map is None:
            inc_map = {}

        return UrlCacheClient._update_url_info(url, update_map, inc_map, cond, True, fields)

    @classmethod
    def find_and_modify_url_info(cls, url, update_map, inc_map, fields):
        """
        found: returns original value
        not_in_cache: returns None
        """

        return UrlCacheClient._find_and_modify_by_cond(url, None, update_map, inc_map, fields)

    @classmethod
    def find_and_modify_url_info_by_status(cls, url, crawl_status, update_map, inc_map, fields):
        """
        found: returns original value
        cond_not_matched: returns False
        not_in_cache: returns None
        """

        cond = {"fields" : ["crawl_status"], "func" : lambda url_info : url_info["crawl_status"] == crawl_status if url_info is not None else False}
        return UrlCacheClient._find_and_modify_by_cond(url, cond, update_map, inc_map, fields)

    @classmethod
    def find_and_modify_url_info_by_not_md5(cls, url, md5, update_map, inc_map, fields):
        """
        returns True/False if update succeeded
        returns False if cond not met
        """

        cond = {"fields" : ["md5"], "func" : lambda url_info : url_info["md5"] != md5 if url_info is not None else False}
        return UrlCacheClient._find_and_modify_by_cond(url, cond, update_map, inc_map, fields)

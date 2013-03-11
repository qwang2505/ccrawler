import robotparser

import ccrawler.utils.misc as misc
import ccrawler.common.settings as common_settings
import ccrawler.utils.object_cache as object_cache

def _get_robot_parser(scheme, host):
    robot_parser = object_cache.get("robot_parser", host)
    if robot_parser is None:
        robots_url = "%s://%s/robots.txt" % (scheme, host)
        robots_file = common_settings.cache_client().get("robots_txt", host)
        if robots_file is None:
            robots_file = misc.load_body(robots_url, encoding="utf-8")#TODO: change this to asynchronous
            if robots_file is None:
                return None
            else:
                common_settings.cache_client().set("robots_txt", host, data = robots_file.encode("utf-8", "ignore"))
        else:
            robots_file = robots_file.decode("utf-8", "ignore")

        robot_parser = robotparser.RobotFileParser(robots_url)
        robot_parser.parse(robots_file.splitlines())
        object_cache.set("robot_parser", host, robot_parser)

    return robot_parser

def allowed_url(url, user_agent, scheme=None, host=None):
    if scheme is None or host is None:
        parsed_result = misc.parse_url(url)
        if parsed_result is None:
            return False
        scheme = parsed_result.scheme
        host = parsed_result.netloc

    robot_parser = _get_robot_parser(scheme, host)
    if robot_parser is not None and not robot_parser.can_fetch(user_agent, url):
        return False
    else:
        return True


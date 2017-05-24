from splunklib.client import connect
from splunklib.results import ResultsReader
from splunktospan import LogParser, DictParser
import re
import sys
import os
import lightstep

try:
    import utils
except ImportError:
    raise Exception("Add the SDK repository to your PYTHONPATH to run the examples "
                    "(e.g., export PYTHONPATH=~/splunk-sdk-python.")

tracers = {}
# for examples of tailing off the splunk API see
# https://github.com/splunk/splunk-sdk-python/blob/master/examples/stail.py
# and https://github.com/splunk/splunk-sdk-python/blob/master/examples/follow.py

def main():
    duration_keys = ["latencymillis", "elapsedmillis", "duration", 'dur', 'latency']
    ls_component_key = "lightstep.component_name"
    join_guid = "guid:correlation_id"
    tags_to_rewrite = {
        "correlation_id": join_guid,
        "cid": join_guid,
        "component": ls_component_key,
        "sourcetype": ls_component_key}


    opts = utils.parse(sys.argv[1:], {}, ".splunkrc", usage="")
    if len(opts.args) != 1:
        utils.error("Search expression required", 2)
    search = opts.args[0]
    service = connect(**opts.kwargs)

    regex = re.compile("(?P<start_time>.+) (?P<component>.+) (?P<operation>.+): (?P<tags>.*)")

    log_parser = LogParser(regex)
    log_parser.duration_keys = duration_keys
    log_parser.downcase_keys = True

    dict_parser = DictParser()
    dict_parser.downcase_keys = True
    dict_parser.operation_keys = ['activity', 'name', 'path']
    dict_parser.duration_keys = duration_keys

    results = service.get(
            "search/jobs/export",
            search=search,
            earliest_time="rt",
            latest_time="rt",
            search_mode="realtime",
            app="tm_home_authoring")

    for result in ResultsReader(results.body):
        try:
            log = None
            # I'm not sure what will actually be used here since ResultReader can return
            # either a dictionary or message. I imagine a dict=>ParsedLog should be
            # straightforward to write.
            # https://github.com/splunk/splunk-sdk-python/blob/master/splunklib/results.py#L170
            if result is None:
                break
            if isinstance(result, dict):
                # print result
                parsed = dict_parser.parse_dict(result)
            else:
                parsed = log_parser.parse_line(result.message)

            parsed.rewrite_tags(tags_to_rewrite)

            if "status" in parsed.tags:
                try:
                    http_status = int(parsed.tags['status'])
                    if http_status >= 200 and http_status < 300:
                        parsed.tags['status_class'] = '2xx'
                    if http_status >= 300 and http_status < 400:
                        parsed.tags['status_class'] = '3xx'
                    if http_status >= 400 and http_status < 500:
                        parsed.tags["error"] = True
                        parsed.tags['status_class'] = '4xx'
                    if http_status >= 500:
                        parsed.tags["error"] = True
                        parsed.tags['status_class'] = '5xx'
                except ValueError:
                    if "FAILURE" == parsed.tags['status']:
                        parsed.tags["error"] = True
                    elif 'error' in parsed.tags['status'].lower():
                        parsed.tags["error"] = True
                    elif "failed" in parsed.tags and parsed.tags["failed"].lower() == "true":
                        parsed.tags["error"] = True


            reservation_prefix = "/rest/unifiedinventory/v1/Reservations/tpi-reserve-"
            reservation_stable_name = "/rest/unifiedinventory/v1/Reservations/tpi-reserve-{id}"

            if parsed.operation_name.startswith(reservation_prefix):
                parsed.operation_name = reservation_stable_name

            component = parsed.tags["sourcetype"]
            if component in tracers:
                parsed.tracer = tracers[component]
            else:
                tracer = lightstep.Tracer(
                    component_name=component,
                    access_token=os.environ['LIGHTSTEP_ACCESS_TOKEN']
                )
                tracers[component] = tracer
                parsed.tracer = tracer

            parsed.to_span()
        except Exception as e:
            print("Did not parse line: ", result, "(error: ", e, ")")

if __name__ == "__main__":
    main()

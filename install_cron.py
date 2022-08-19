import optparse
import os
import subprocess
import toml

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", default="explorer.toml", dest="config_file")
    options, args = parser.parse_args()

    config = toml.load(options.config_file)
    caching_scripts = config["api"]["caching"]["scripts"]

    cron_config = {}
    for script in caching_scripts:
        cron_config[script] = config["api"]["caching"]["scripts"][script]["cron"]

    p = subprocess.Popen(["crontab", "-l"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    cron_lines = [line.decode("utf-8").strip() for line in stdout.splitlines()]
    if len(cron_lines) > 0:
        cron_lines.append("")

    explorer = "/home/witnet/explorer"
    backend = f"{explorer}/backend"
    for cache_process, config in cron_config.items():
        if config == "* * * * *":
            time_indication = "every minute"
        elif config == "0 * * * *":
            time_indication = "at the top of every hour"
        else:
            time_indication = ""

        cron_lines.append(
            f"# Execute the {cache_process} caching process {time_indication}. Use flock to prevent concurrent execution."
        )
        cron_lines.append(
            f"{config} cd {backend} && flock -n {backend}/caching/.{cache_process}.lock {explorer}/env/bin/python3 -m caching.{cache_process} --config-file {backend}/explorer.toml\n"
        )

    f = open("crontabs.txt", "w+")
    f.write("\n".join(cron_lines))
    f.close()

    p = subprocess.Popen(["crontab", "crontabs.txt"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()
    assert len(stdout) == 0 and len(stderr) == 0

    os.remove("crontabs.txt")

if __name__ == "__main__":
    main()

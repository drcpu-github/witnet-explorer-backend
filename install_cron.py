import optparse
import os
import subprocess
import toml

def translate_cron(config):
    if config == "* * * * *":
        return "every minute"
    elif config[:2] == "*/" and config[-8:] == "* * * *":
        minutes = config.split()[2:]
        return f"every {minutes} minutes"
    elif config == "0 * * * *":
        return "at the top of every hour"
    else:
        return ""

def main():
    parser = optparse.OptionParser()
    parser.add_option("--config-file", type="string", default="explorer.toml", dest="config_file")
    options, args = parser.parse_args()

    config = toml.load(options.config_file)
    caching_scripts = config["api"]["caching"]["scripts"]

    cron_config = {}
    for script in caching_scripts:
        if "cron" in config["api"]["caching"]["scripts"][script]:
            cron_config[script] = config["api"]["caching"]["scripts"][script]["cron"]

    p = subprocess.Popen(["crontab", "-l"], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = p.communicate()

    cron_lines = [line.decode("utf-8").strip() for line in stdout.splitlines()]
    if len(cron_lines) > 0:
        cron_lines.append("")

    explorer = config["explorer"]["path"]
    backend = f"{explorer}/backend"

    # Cron jobs for all processes caching data
    for cache_process, cron in cron_config.items():
        time_indication = translate_cron(cron)
        cron_lines.append(
            f"# Execute the {cache_process} caching process {time_indication}. Use flock to prevent concurrent execution."
        )
        cron_lines.append(
            f"{cron} cd {backend} && flock -n {backend}/caching/.{cache_process}.lock {explorer}/env/bin/python3 -m caching.{cache_process} --config-file {backend}/explorer.toml\n"
        )

    # Cron job for the TRS building process
    cron = config["engine"]["cron"]
    time_indication = translate_cron(cron)
    cron_lines.append(
        f"# Execute the TRS building process {time_indication}. Use flock to prevent concurrent execution."
    )
    cron_lines.append(
        f"{cron} cd {backend} && flock -n {backend}/engine/.reputation.lock {explorer}/env/bin/python3 -m engine.reputation --config-file {backend}/explorer.toml --load-trs --persist-trs\n"
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

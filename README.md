# RealtimeCosts

This system was set up so that IDC can provide sponsored GCP projects, each with a specified total budget, to researchers.
All projects are attached to the same single billing account, so this system monitors the spending of each project
individually to make sure each one stays within the set spending limit. Each project is set up with a
separate per-project budget, but each budget sends a pub/sub message to a single monitoring cloud function.

The routine ProjectCostControlWithEstimates.py is intended to monitor costs in a GCP project "A" by being installed
in a separate project "B". A budget alert is established in the billing account attached to project "A", and this
alert publishes to a Pub/Sub topic set up in project "B". Messages are sent about every twenty minutes to the topic.

In project "B", the function receives the message, which contains the current spending for the project, and uses
this trigger to determine if the project needs to be shut down in any fashion. Using example code snippets,
provided by Google, it shuts down all VMs and all AppEngine services if the budget has been reached. If the
spending exceeds some multiplier (e.g. 1.2 times the budget), it pulls the billing account off the project, which
halts all spending, at the risk of losing resources if the account is not restored in a timely fashion.

Note that to be able to perform these actions, the service account in project "B" running the function needs
to have "Editor" and "Project Billing Manager" roles in project "A". Editor is certainly too broad a role,
and could be reduced to a custom role with some analysis [TODO].

In addition to writing to logs in project "B", each run reports spending to a log in project "A" that can be
monitored by members of project "A". Set an advanced filter in Stackdriver in project "A" to see these messages:

```
logName="projects/project-a-id/logs/cost_estimation_log"
```

Note that the log messages issued in project "B" are set up with the intention of having project monitoring
in project "B" trigger alerts with emails or text messages to monitor the system.

This function was developed before Google allowed custom ranges to be specified for budgets, and assumes the
budget alert is monthly, but that the budget set specifies the "all-time" spending for the project. Thus, it
keeps state in a bucket in project "B" that you specify. One gotcha is that it assumes an empty file for project "A"
already exists in the bucket. (still a to-do).  Note that each project has a separate file, since the function
is being triggered by each separate project, and we want to avoid race conditions. (Note the function usuall takes
10-20 seconds to run for each project it processes). With enough projects to monitor, it might fall behind (?). This
has not been explored.

In addition to monitoring the overall spending reported, the function checks if reported egress out of the cloud
exceeds a specified fraction of the budget, since this has been an issue seen with some sponsored projects created
for new cloud users. In this case, it just issues a log message alert, but does nothing else.

Note that monitoring egress spending requires the billing account to have a BigQuery table set up to receive the
current spending data from Google. Google does not currently have an API to query about amounts spent,
so the BigQuery export is the only way to get the current fine-grained spending data needed to estimate egress
charges.

Another thing the system checks is the number of running CPUs. While Google does allow you to reduce quotas for
VM counts in projects, this can take awhile and needs human intervention by Google support. Thus, this function
provides another way to cap VM usage. It counts the number of CPUs running in the project, and if that exceeds
a configurable value, the function will shut down VMs in a LIFO fashion to get back under the limit.

Finally, one of the big shortcomings of the above system is that the published spending numbers can lag actual
expense by 12 hours (give or take). So shutting a project down based on stale numbers may be too late to avoid
unexpected charges. Thus, this function also estimates the current burn rate for running VMs and persistent storage
each time the function runs (e.g. every 20 minutes). If the system detects that existing reported spending, plus
12 hours of this estimated burn, will exceed the budget by a specified multiplier, it will shut down all running VMs.

To achieve this, a set of BigQuery tables need to be created in project "B" that quantify the costs associated
with running VMs. The seed for these tables is the SKU export table that Google will create if this export is
specified for the billing account. With that table in hand, running the function "extract-sku-tables-from-desktop.sh"
pointed at that table will generate all the specified SKU tables needed by the cloud function to do this estimation.
Note this routine uses the standard method used by the ISB-CGC ETL system, where config files to run the script
live in a cloud configuration bucket. Usually those scripts run on a VM, but the "-from-desktop" bit means this
script is set up to run on your local laptop, usually after running "gcloud auth application-default login"
to get the script to run locally using personal credentials. (Remember to run "gcloud auth application-default revoke"
when done to get back to normal service account-driven script execution.

Quantifying this spending exactly is a work in progress, and currently involves educated guesswork as to how
a CPU tag maps to specific CPU and RAM SKUs. E.g., it appears that "c2-standard" might map to "Compute optimized Core"
and "Compute optimized Ram". But only a small set of the search space has been confirmed; the rest is educated
guessing.

The problem is even more complex for OS licensing, especially for Windows machines. The system is currently
configured to try and handle the various cases (e.g. OS licensing costs based on GPU count). All pull requests
to improve the mapping tables are welcome.

Finally, these price estimates do not include tiered pricing and discounts (or external IP charges, yet). It
is intended to be conservative. But these enhancements would be useful.

Note that the cloud function can be triggered from an HTTP entry point if you just want to run the realtime
estimates. To track a budget and (maybe) shut down resources, the Pub/Sub entry is the one to use.





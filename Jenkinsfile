#!groovy

@Library('katsdpjenkins') _
katsdp.killOldJobs()
katsdp.setDependencies(['ska-sa/katsdpdockerbase/master'])
katsdp.standardBuild(python3: true, push_external: true)
katsdp.mail('sdpdev+katsdptelstate@ska.ac.za')

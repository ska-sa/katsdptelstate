#!groovy

@Library('katsdpjenkins@python2') _
katsdp.killOldJobs()
katsdp.setDependencies(['ska-sa/katsdpdockerbase/python2'])
katsdp.standardBuild(python3: true, push_external: true,
                     katsdpdockerbase_ref: 'python2')
katsdp.mail('sdpdev+katsdptelstate@ska.ac.za')

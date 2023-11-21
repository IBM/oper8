# Oper8 Contributor Guide

Welcome to Oper8! This document is the single source of truth for how to contribute to the code base. Feel free to browse the [open issues](https://github.com/IBM/oper8/issues) and file new ones, all feedback welcome!

# Before you get started

## Sign the DCO

The sign-off is a simple line at the end of the explanation for a commit. All commits needs to be signed. Your signature certifies that you wrote the patch or otherwise have the right to contribute the material. The rules are pretty simple, if you can certify the below (from [developercertificate.org](https://developercertificate.org/)):

```
Developer Certificate of Origin
Version 1.1

Copyright (C) 2004, 2006 The Linux Foundation and its contributors.
1 Letterman Drive
Suite D4700
San Francisco, CA, 94129

Everyone is permitted to copy and distribute verbatim copies of this
license document, but changing it is not allowed.

Developer's Certificate of Origin 1.1

By making a contribution to this project, I certify that:

(a) The contribution was created in whole or in part by me and I
    have the right to submit it under the open source license
    indicated in the file; or

(b) The contribution is based upon previous work that, to the best
    of my knowledge, is covered under an appropriate open source
    license and I have the right under that license to submit that
    work with modifications, whether created in whole or in part
    by me, under the same open source license (unless I am
    permitted to submit under a different license), as indicated
    in the file; or

(c) The contribution was provided directly to me by some other
    person who certified (a), (b) or (c) and I have not modified
    it.

(d) I understand and agree that this project and the contribution
    are public and that a record of the contribution (including all
    personal information I submit with it, including my sign-off) is
    maintained indefinitely and may be redistributed consistent with
    this project or the open source license(s) involved.
```

Then you just add a line to every git commit message:

    Signed-off-by: Joe Smith <joe.smith@example.com>

Use your real name (sorry, no pseudonyms or anonymous contributions.)

If you set your `user.name` and `user.email` git configs, you can sign your commit automatically
with `git commit -s`.

Note: If your git config information is set properly then viewing the `git log` information for your
commit will look something like this:

```
Author: Joe Smith <joe.smith@example.com>
Date:   Thu Feb 2 11:41:15 2018 -0800

    Update README

    Signed-off-by: Joe Smith <joe.smith@example.com>
```

Notice the `Author` and `Signed-off-by` lines match. If they don't your PR will be rejected by the
automated DCO check.

## Code attribution

License information should be included in all source files where applicable. Either full or short version of the header should be used as described at [apache.org](http://www.apache.org/foundation/license-faq.html#Apply-My-Software). It is OK to exclude the year from the copyright notice. For the details on how to apply the copyright, see the next section.

## Copyright Notices

Oper8 used "Copyright The Oper8 Authors" notice form.

If you are contributing third-party code you will need to retain the original copyright notice.

Any contributed third-party code must originally be Apache 2.0-Licensed or must carry a permissive software license that is compatible when combining with Apache 2.0 License. At this moment, BSD and MIT are the only [OSI-approved licenses](https://opensource.org/licenses/alphabetical) known to be compatible.

If you make substantial changes to the third-party code, _prepend_ the contributed third party file with Oper8's copyright notice.

If the contributed code is not third-party code and you are the author we strongly encourage to avoid including your name in the notice and use the generic "Copyright The Oper8 Authors" notice.

## Code of Conduct

Please make sure to read and observe our [Code of Conduct](./CODE_OF_CONDUCT.md).

# Your First Contribution

Would you like to help drive the community forward? We will help you understand the organization of the project and direct you to the best places to get started. You'll be able to pick up issues, write code to fix them, and get your work reviewed and merged.

## Find something to work on

Help is always welcome! For example, documentation (like the text you are reading now) can always use improvement. There's always code that can be clarified and variables or functions that can be renamed or commented. There's always a need for more test coverage. You get the idea: if you ever see something you think should be fixed, you should own it.

Those interested in contributing without writing code may help documenting, evangelizing or helping answer questions about Oper8 on various forums.

### File an Issue

Not ready to contribute code, but see something that needs work? While the community encourages everyone to contribute code, it is also appreciated when someone reports an issue (aka problem).

### Contributing

Oper8 is open source, but many of the people working on it do so as their day job. In order to avoid forcing people to be "at work" effectively 24/7, we want to establish some semi-formal protocols around development. Hopefully, these rules make things go more smoothly. If you find that this is not the case, please complain loudly.

As a potential contributor, your changes and ideas are welcome at any hour of the day or night, weekdays, weekends, and holidays. Please do not ever hesitate to ask a question or send a pull request.

## GitHub workflow

To check out code to work on, please refer to [the GitHub Workflow Guide](https://github.com/kubernetes/community/blob/master/contributors/guide/github-workflow.md) from Kubernetes. Oper8 uses the same workflow. One of the main highlights - all the work should happen on forks, to minimize the number of branches on a given repository.

## Open a Pull Request

Pull requests are often called simply "PR". Oper8 follows the standard [github pull request](https://help.github.com/articles/about-pull-requests/) process.

## Code Review

There are two aspects of code review: giving and receiving.

To make it easier for your PR to receive reviews, consider the reviewers will need you to:

- follow the project and repository coding conventions
- write [good commit messages](https://chris.beams.io/posts/git-commit/)
- break large changes into a logical series of smaller patches which individually make easily understandable changes, and in aggregate solve a broader issue

Reviewers, the people giving the review, are highly encouraged to revisit the [Code of Conduct](./CODE_OF_CONDUCT.md) and must go above and beyond to promote a collaborative, respectful community.

When reviewing PRs from others [The Gentle Art of Patch Review](http://sage.thesharps.us/2014/09/01/the-gentle-art-of-patch-review/) suggests an iterative series of focuses which is designed to lead new contributors to positive collaboration without inundating them initially with nuances:

- Is the idea behind the contribution sound?
- Is the contribution architected correctly?
- Is the contribution polished?

Note: if your pull request isn't getting enough attention, you can explicitly mention approvers or maintainers of this repository.

.. SPDX-FileCopyrightText: 2021-2022 Constantine Evans <const@costi.eu>
..
.. SPDX-License-Identifier: AGPL-3.0-only

Setup
=====

Access
------

The QuantStudio machine communicates in two ways, depending on firmware version:

- Firmware version 1.3.1 and earlier: unencrypted, via a SCPI interface on port 7000.
- Firmware version 1.3.2 and later: encrypted, via a SCPI interface on port 7443, but using a self-signed certificate and SSLv3.

The machine has a password authentication mechanism.  However, it is extremely insecure, and should not be relied on for security (see :ref:`access-and-security`).  Each password has an associated access level, which determines what functions can be accessed.  There are five access levels: Guest, Observer, Controller, Administrator, and Full.  While QSLib supports all of these, only two are usually useful:

- Observer provides access to most functions giving a read-only view of the machine and experiments.
- Controller allows control of the machine's functions and experiments.

Guest appears to be intended to provide access only to the help system and authentication, while higher access levels are related to programming the machine.

Configuration suggestions
-------------------------

One reasonably secure configuration:

- The QuantStudio machine is connected, via an ethernet cable, directly to a laptop, or otherwise to a secure private network.  Anyone with access to this network can easily gain full access to the machines, regardless of configuration.  Thus, it makes sense to allow Observer and Controller access without a password (putting higher access levels behind a password to prevent mistakes, not to secure the machines).  Wifi should be turned off in most circumstances.

- The laptop, or a device on both the private network and a larger network or the internet, securely allows access to port 7000 on the QuantStudio machine.  This could be through port forwarding and a VPN, or through an SSH tunnel.

QSLib provides a CLI command, :code:`qslib setup-machine`, which will configure a machine to have the default, more convenient access useful with this configuration.

.. _access-and-security:

Security Considerations
-----------------------

At its base, the QuantStudio 5 has an ARM-based system running Android. It uses a standard network connection through its ethernet port, and has an SCPI interface on port 7000 for non-SSL connections, and 7443 for SSL connections.  On later firmware versions, only 7443 is accessible from outside the machine.  The system's extensive and admirable use of standard and open formats and software, as well as its extensive and helpful on-machine documentation, makes it possible for the machine to be adapted for other uses beyond qPCR.

Access to the machine is based on passwords, which could be seen as access keys: each password is associated with a maximum access level.  In order to use QSLib, you'll need passwords for your device: most functions require a password with either :term:`Observer` or :term:`Controller` level access.  Prior to firmware version 1.3.2, the machine relied on default passwords.  After 1.3.4, the machine switched to allowing a "Secure Secret" to be set by the machine administrator in the android-based UI, which is actually a Controller-level password.  The is the "OEM Connection Only" function.

Nevertheless, **I strongly recommend that the machines not be accessible on public networks:**

* **Anyone with network access to the machne can easily obtain :term:`Full` access to the SCPI interface and root shell access to the system, even if passwords and access levels on the system have been changed from their defaults.**  This **cannot be mitigated** through configuration changes on the machine, like passwords or access levels.  This extends beyond the machine itself: an attacker with access to the machine could use it as a general Linux system for other purposes, including spying on or attacking your network.  Access levels and passwords should be considered as protecting only against unintended interference and user mistakes.  The vulnerabilities are not mitigated by SSL connections. [#security]_

* Prior to firmware version 1.3.4, while passwords are hashed and are not sent cleartext during authentication, all communication with the device is unencrypted and unsigned.  Additionally, while I have not investigated it, the android interface on the machine attempts to make non-SSL HTTP connections to hostnames that could be vulnerable to DNS spoofing attacks.  For version 1.3.4 and above, while SSL is used, it is SSLv3, which has the POODLE vulnerability is not secure.

In our setup, we have machines directly connected to a dedicated ethernet interface on controlling laptops, with no direct internet connection, outbound or inbound, from the machine.  The laptops are connected to a VPN, and forward connections from the VPN to port 7000 on the machine.  This setup allows QSLib access via VPN and AB software access from the laptop.

.. [#otherport] The machine also listens on port 2323, where it has a simple console interface designed for manual use.
.. [#security] It is possible that the machine could be security-hardened with some modifications, and I would be happy to discuss this.

.. glossary::

    Observer
        Read-only access level.
    Controller
        Access level allowing control of the machine, but not administrative functions.

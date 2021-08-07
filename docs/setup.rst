.. _setup:

Setup
=====


.. _access-and-security:

Access and Security Considerations
----------------------------------

At its base, the QuantStudio 5 has an ARM-based system running Android. It uses a standard network connection through its ethernet port, and has an SCPI interface on port 7000. [#otherport]_ The system's extensive and admirable use of standard and open formats and software, as well as its extensive and helpful on-machine documentation, makes it possible for the machine to be adapted for other uses beyond qPCR.

Access to the machine is based on passwords, which could be seen as access keys: each password is associated with a maximum access level.  In order to use QSLib, you'll need passwords for your device: most functions require a password with either :term:`Observer` or :term:`Controller` level access.  **QSLib does not contain any default passwords, or mechanisms for obtaining access to arbitrary machines.**

Nevertheless, **I strongly recommend that the machines not be accessible on public networks:**

* **Anyone with network access to the machne can easily obtain :term:`Full` access to the SCPI interface and root shell access to the system, even if passwords and access levels on the system have been changed from their defaults.**  This **cannot be mitigated** through configuration changes on the machine. [#security]_

* To reiterate: any non-modified machine can be fully accessed by anyone with access to port 7000 or 2323, **including root access and a root shell on the machine**, regardless of passwords or access levels.  This extends beyond the machine itself: an attacker with access to the machine could use it as a general Linux system for other purposes, including spying on or attacking your network.  Access levels and passwords should be considered as protecting only against unintended interference and user mistakes.

* While passwords are hashed and are not sent cleartext during authentication, all communication with the device is unencrypted and unsigned.  Additionally, while I have not investigated it, the android interface on the machine attempts to make non-SSL HTTP connections to hostnames that could be vulnerable to DNS spoofing attacks.

In our setup, we have machines directly connected to a dedicated ethernet interface on controlling laptops, with no direct internet connection, outbound or inbound, from the machine.  The laptops are connected to a VPN, and forward connections from the VPN to port 7000 on the machine.  This setup allows QSLib access via VPN and AB software access from the laptop.

.. [#otherport] The machine also listens on port 2323, where it has a simple console interface designed for manual use.
.. [#security] It is possible that the machine could be security-hardened with some modifications, and I would be happy to discuss this.

.. glossary::

    Observer
        Read-only access level.
    Controller
        Access level allowing control of the machine, but not administrative functions.
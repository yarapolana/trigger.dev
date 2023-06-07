import {
  ExclamationCircleIcon,
  ExclamationTriangleIcon,
  InformationCircleIcon,
  StopIcon,
} from "@heroicons/react/20/solid";
import { CompanyIcon, hasIcon } from "@trigger.dev/companyicons";
import {
  ArrowLeftIcon,
  ArrowRightIcon,
  BeakerIcon,
  BellAlertIcon,
  BookOpenIcon,
  BuildingOffice2Icon,
  CalendarDaysIcon,
  ChatBubbleLeftEllipsisIcon,
  CheckIcon,
  ChevronLeftIcon,
  ChevronRightIcon,
  ClockIcon,
  CodeBracketSquareIcon,
  Cog8ToothIcon,
  CreditCardIcon,
  EnvelopeIcon,
  FingerPrintIcon,
  FlagIcon,
  FolderIcon,
  GlobeAltIcon,
  HandRaisedIcon,
  HeartIcon,
  KeyIcon,
  LightBulbIcon,
  MagnifyingGlassIcon,
  PlusIcon,
  PlusSmallIcon,
  SquaresPlusIcon,
  StarIcon,
  UserCircleIcon,
  UserGroupIcon,
  UserIcon,
  UserPlusIcon,
  WrenchScrewdriverIcon,
  XMarkIcon,
} from "@heroicons/react/24/solid";
import { ScheduleIcon } from "~/assets/icons/ScheduleIcon";
import { WebhookIcon } from "~/assets/icons/WebhookIcon";
import { cn } from "~/utils/cn";
import { LogoIcon } from "../LogoIcon";
import { Spinner } from "./Spinner";

const icons = {
  account: (className: string) => (
    <UserCircleIcon className={cn("text-slate-400", className)} />
  ),
  "arrow-right": (className: string) => (
    <ArrowRightIcon className={cn("text-white", className)} />
  ),
  "arrow-left": (className: string) => (
    <ArrowLeftIcon className={cn("text-white", className)} />
  ),
  beaker: (className: string) => (
    <BeakerIcon className={cn("text-purple-500", className)} />
  ),
  billing: (className: string) => (
    <CreditCardIcon className={cn("text-teal-500", className)} />
  ),
  calendar: (className: string) => (
    <CalendarDaysIcon className={cn("text-purple-500", className)} />
  ),
  check: (className: string) => (
    <CheckIcon className={cn("text-dimmed", className)} />
  ),
  "chevron-left": (className: string) => (
    <ChevronLeftIcon className={cn("text-dimmed", className)} />
  ),
  "chevron-right": (className: string) => (
    <ChevronRightIcon className={cn("text-dimmed", className)} />
  ),
  clock: (className: string) => (
    <ClockIcon className={cn("text-cyan-500", className)} />
  ),
  close: (className: string) => (
    <XMarkIcon className={cn("text-dimmed", className)} />
  ),
  "connection-alert": (className: string) => (
    <BellAlertIcon className={cn("text-rose-500", className)} />
  ),
  docs: (className: string) => (
    <BookOpenIcon className={cn("text-slate-400", className)} />
  ),
  error: (className: string) => (
    <ExclamationCircleIcon className={cn("text-rose-500", className)} />
  ),
  flag: (className: string) => (
    <FlagIcon className={cn("text-sky-500", className)} />
  ),
  folder: (className: string) => (
    <FolderIcon className={cn("text-indigo-600", className)} />
  ),
  envelope: (className: string) => (
    <EnvelopeIcon className={cn("text-cyan-500", className)} />
  ),
  environment: (className: string) => (
    <KeyIcon className={cn("text-yellow-500", className)} />
  ),
  globe: (className: string) => (
    <GlobeAltIcon className={cn("text-fuchsia-600", className)} />
  ),
  "hand-raised": (className: string) => (
    <HandRaisedIcon className={cn("text-amber-400", className)} />
  ),
  heart: (className: string) => (
    <HeartIcon className={cn("text-rose-500", className)} />
  ),
  id: (className: string) => (
    <FingerPrintIcon className={cn("text-rose-200", className)} />
  ),
  info: (className: string) => (
    <InformationCircleIcon className={cn("text-blue-500", className)} />
  ),
  integration: (className: string) => (
    <SquaresPlusIcon className={cn("text-teal-500", className)} />
  ),
  "invite-member": (className: string) => (
    <UserPlusIcon className={cn("text-indigo-500", className)} />
  ),
  job: (className: string) => (
    <WrenchScrewdriverIcon className={cn("text-teal-500", className)} />
  ),
  key: (className: string) => (
    <KeyIcon className={cn("text-amber-400", className)} />
  ),
  lightbulb: (className: string) => (
    <LightBulbIcon className={cn("text-amber-400", className)} />
  ),
  log: (className: string) => (
    <ChatBubbleLeftEllipsisIcon className={cn("text-slate-400", className)} />
  ),
  organization: (className: string) => (
    <BuildingOffice2Icon className={cn("text-fuchsia-600", className)} />
  ),
  search: (className: string) => (
    <MagnifyingGlassIcon className={cn("text-dimmed", className)} />
  ),
  plus: (className: string) => (
    <PlusIcon className={cn("text-green-600", className)} />
  ),
  "plus-small": (className: string) => (
    <PlusSmallIcon className={cn("text-green-600", className)} />
  ),
  property: (className: string) => (
    <Cog8ToothIcon className={cn("text-slate-600", className)} />
  ),
  spinner: (className: string) => (
    <Spinner className={className} color="blue" />
  ),
  "spinner-white": (className: string) => (
    <Spinner className={className} color="white" />
  ),
  star: (className: string) => (
    <StarIcon className={cn("text-yellow-500", className)} />
  ),
  team: (className: string) => (
    <UserGroupIcon className={cn("text-blue-500", className)} />
  ),
  "logo-icon": (className: string) => <LogoIcon className={cn(className)} />,
  user: (className: string) => (
    <UserIcon className={cn("text-blue-600", className)} />
  ),
  warning: (className: string) => (
    <ExclamationTriangleIcon className={cn("text-amber-400", className)} />
  ),
  //triggers
  "custom-event": (className: string) => (
    <CodeBracketSquareIcon className={cn("text-toxic-600", className)} />
  ),
  "schedule-interval": (className: string) => (
    <ScheduleIcon className={cn("text-sky-500", className)} />
  ),
  "schedule-cron": (className: string) => (
    <ScheduleIcon className={cn("text-sky-500", className)} />
  ),
  "schedule-dynamic": (className: string) => (
    <ScheduleIcon className={cn("text-sky-500", className)} />
  ),
  webhook: (className: string) => (
    <WebhookIcon className={cn("text-pink-500", className)} />
  ),
};

export type IconNames = keyof typeof icons;
export const iconNames = Object.keys(icons) as IconNames[];

export function NamedIcon({
  name,
  className,
  fallback,
}: {
  name: string;
  className: string;
  fallback?: JSX.Element;
}) {
  if (Object.keys(icons).includes(name)) {
    return icons[name as IconNames](className);
  }

  if (hasIcon(name)) {
    return (
      <div
        className={cn(
          "grid aspect-square min-h-fit place-items-center",
          className
        )}
      >
        <CompanyIcon
          name={name}
          className={"h-full w-full p-[7%]"}
          variant="light"
        />
      </div>
    );
  }

  console.log(`Icon ${name} not found`);

  if (fallback) {
    return fallback;
  }

  //default fallback icon
  return <StopIcon className={className} />;
}

export function NamedIconInBox({
  name,
  className,
  fallback,
}: {
  name: string;
  className?: string;
  fallback?: JSX.Element;
}) {
  return (
    <div
      className={cn(
        "grid place-content-center rounded-sm border border-slate-750 bg-slate-850",
        className
      )}
    >
      <NamedIcon name={name} fallback={fallback} className="h-6 w-6" />
    </div>
  );
}
